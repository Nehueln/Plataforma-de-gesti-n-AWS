#!/usr/bin/env python3
"""
custom_table_manager.py
Lógica de negocio para la gestión de Custom Tables en DynamoDB:
- Creación de tabla con partition key 'custom_key' y carga masiva desde Excel / CSV.
- Editor y validación dinámica de REQUIRED_COLUMNS.
- Detección de columnas faltantes, claves vacías y claves duplicadas.
- Listado de tablas DynamoDB y vaciado de tablas.
"""

import io
import os
import re
import time
import boto3
import pandas as pd
from botocore.exceptions import ClientError
from promote_config import ENVS

DEFAULT_COLUMNS = [
    'custom_key', 'alias_display', 'cliente', 'dni', 'tipo_contacto', 'deuda'
]


def get_dynamodb_resource(env='desa'):
    """Obtiene el resource de DynamoDB para el ambiente indicado."""
    if env in ENVS:
        cfg = ENVS[env]
        session = boto3.Session(profile_name=cfg["profile"], region_name=cfg["region"])
        return session.resource('dynamodb')
    return boto3.resource('dynamodb')


def get_dynamodb_client(env='desa'):
    """Obtiene el client de DynamoDB para el ambiente indicado."""
    if env in ENVS:
        cfg = ENVS[env]
        session = boto3.Session(profile_name=cfg["profile"], region_name=cfg["region"])
        return session.client('dynamodb')
    return boto3.client('dynamodb')


def get_existing_table(table_name, env='desa'):
    """
    Obtiene una tabla existente en DynamoDB en el ambiente especificado.
    Si la tabla NO existe en DynamoDB, lanza ValueError con mensaje explícito.
    """
    table_name = table_name.strip()
    dynamodb = get_dynamodb_resource(env)
    table = dynamodb.Table(table_name)
    try:
        table.load()
        return table
    except ClientError as e:
        if e.response['Error']['Code'] in ('ResourceNotFoundException', 'ValidationException'):
            raise ValueError(f"La tabla '{table_name}' no existe en el ambiente {env.upper()}. La tabla debe existir previamente en DynamoDB para poder cargar datos.")
        raise


def ensure_table_exists(table_name, env='desa'):
    """
    Valida que la tabla exista en DynamoDB sin crearla automáticamente.
    Retorna (table, created_now=False).
    """
    table = get_existing_table(table_name, env=env)
    return table, False


def load_dataframe(file_input, filename=""):
    """
    Carga un DataFrame a partir de una ruta de archivo, bytes o un buffer io.BytesIO.
    Soporta formatos .xlsx, .xls y .csv.
    """
    if isinstance(file_input, str):
        path_lower = file_input.lower()
        if path_lower.endswith(('.xlsx', '.xls')):
            return pd.read_excel(file_input)
        return pd.read_csv(file_input)

    # Si es bytes, envolver en BytesIO
    if isinstance(file_input, bytes):
        buffer = io.BytesIO(file_input)
    else:
        buffer = file_input

    fname_lower = filename.lower() if filename else ""
    if fname_lower.endswith(('.xlsx', '.xls')):
        return pd.read_excel(buffer)
    elif fname_lower.endswith('.csv'):
        return pd.read_csv(buffer)
    else:
        # Intentar excel primero, fallback a csv
        try:
            return pd.read_excel(buffer)
        except Exception:
            buffer.seek(0)
            return pd.read_csv(buffer)


def inspect_file_preview(file_input, configured_columns=None, filename=""):
    """
    Inspecciona el archivo Excel/CSV y compara sus encabezados con las columnas configuradas.
    Retorna métricas, detección de custom_key, claves vacías, duplicadas y muestra de primeros registros.
    """
    if configured_columns is None:
        configured_columns = list(DEFAULT_COLUMNS)

    # Asegurar que custom_key esté siempre al inicio de las configuradas si no está
    if 'custom_key' not in configured_columns:
        configured_columns = ['custom_key'] + [c for c in configured_columns if c != 'custom_key']

    df = load_dataframe(file_input, filename=filename)
    file_columns = [str(c).strip() for c in df.columns]
    total_rows = len(df)

    missing_columns = [c for c in configured_columns if c not in file_columns]
    matched_columns = [c for c in configured_columns if c in file_columns]

    has_custom_key = 'custom_key' in file_columns
    empty_keys = 0
    duplicate_keys = 0

    if has_custom_key:
        keys_series = df['custom_key'].fillna("").astype(str).str.strip()
        empty_keys = int((keys_series == "").sum())
        valid_keys = keys_series[keys_series != ""]
        duplicate_keys = int(valid_keys.duplicated().sum())

    # Generar muestra de primeros registros (hasta 10)
    cols_to_preview = matched_columns if matched_columns else file_columns[:8]
    preview_df = df[cols_to_preview].head(10).copy()
    for col in preview_df.columns:
        preview_df[col] = preview_df[col].fillna("").astype(str).str.strip()

    preview_rows = preview_df.to_dict(orient="records")

    return {
        "filename": filename or os.path.basename(file_input) if isinstance(file_input, str) else "archivo_cargado",
        "total_rows": total_rows,
        "file_columns": file_columns,
        "configured_columns": configured_columns,
        "matched_columns": matched_columns,
        "missing_columns": missing_columns,
        "has_custom_key": has_custom_key,
        "empty_keys": empty_keys,
        "duplicate_keys": duplicate_keys,
        "preview_rows": preview_rows
    }


def process_and_upload(table, file_input, required_columns=None, filename=""):
    """
    Ejecuta el procesamiento y carga en lote a DynamoDB.
    - Valida que todas las required_columns existan en el archivo.
    - Limpia NaNs convirtiendo a string.
    - O parasite valores vacíos.
    - Realiza batch_writer omitiendo registros sin custom_key.
    """
    if required_columns is None:
        required_columns = list(DEFAULT_COLUMNS)

    df = load_dataframe(file_input, filename=filename)

    # 1. Validar columnas requeridas
    file_cols = [str(c).strip() for c in df.columns]
    # Mapear columnas del dataframe a nombres limpios si tienen espacios
    df.columns = file_cols
    missing = [c for c in required_columns if c not in df.columns]
    if missing:
        raise ValueError(f"Faltan las siguientes columnas en el archivo: {missing}")

    if 'custom_key' not in required_columns:
        raise ValueError("La columna 'custom_key' es obligatoria como clave de partición.")

    # 2. Filtrar exclusivamente las columnas configuradas por el usuario
    df_to_upload = df[required_columns].copy()

    # 3. Limpieza de datos (DynamoDB no acepta NaNs)
    for col in required_columns:
        df_to_upload[col] = df_to_upload[col].fillna("").astype(str).str.strip()

    # 4. Carga masiva con batch_writer
    count = 0
    skipped_empty = 0
    with table.batch_writer() as batch:
        for _, row in df_to_upload.iterrows():
            item = {k: v for k, v in row.to_dict().items() if v != ""}
            if 'custom_key' not in item or not item['custom_key']:
                skipped_empty += 1
                continue
            batch.put_item(Item=item)
            count += 1

    return {
        "status": "success",
        "total_rows": len(df),
        "uploaded_records": count,
        "skipped_empty_keys": skipped_empty,
        "columns": required_columns
    }


def execute_create_and_upload(table_name, file_input, required_columns=None, env='desa', filename=""):
    """
    Flujo completo:
    1. Valida el nombre de tabla y parámetros.
    2. Asegura que la tabla exista o la crea en DynamoDB en el ambiente seleccionado.
    3. Carga y sube los datos configurados con process_and_upload.
    """
    start_time = time.time()
    table_name = table_name.strip()
    if not table_name:
        raise ValueError("El nombre de la tabla no puede estar vacío.")

    if not re.match(r'^[a-zA-Z0-9_.-]{3,255}$', table_name):
        raise ValueError("El nombre de la tabla debe tener entre 3 y 255 caracteres y contener solo letras, números, guiones y guiones bajos.")

    if required_columns is None:
        required_columns = list(DEFAULT_COLUMNS)

    # Validar columnas no vacías y sin duplicados
    clean_cols = []
    seen = set()
    for c in required_columns:
        c_str = str(c).strip()
        if not c_str:
            raise ValueError("No se permiten nombres de columna vacíos.")
        if c_str in seen:
            raise ValueError(f"Columna duplicada: '{c_str}'. Cada columna debe tener un nombre único.")
        seen.add(c_str)
        clean_cols.append(c_str)

    if 'custom_key' not in clean_cols:
        raise ValueError("La columna 'custom_key' es obligatoria.")

    # Validar que la tabla exista en DynamoDB en el ambiente seleccionado
    table = get_existing_table(table_name, env=env)

    # Procesar archivo y subir registros a la tabla existente
    result = process_and_upload(table, file_input, required_columns=clean_cols, filename=filename)
    result["table_name"] = table_name
    result["env"] = env
    result["created_table"] = False
    result["duration_seconds"] = round(time.time() - start_time, 2)
    return result


# Alias semántico
execute_load_table = execute_create_and_upload


def list_dynamodb_tables(env='desa'):
    """
    Lista las tablas DynamoDB disponibles en el ambiente seleccionado con sus detalles de clave.
    """
    dynamodb = get_dynamodb_client(env)
    response = dynamodb.list_tables()
    table_names = response.get('TableNames', [])

    tables_info = []
    for name in sorted(table_names):
        info = {
            "name": name,
            "partition_key": None,
            "sort_key": None,
            "item_count": None
        }
        try:
            desc = dynamodb.describe_table(TableName=name).get('Table', {})
            info["item_count"] = desc.get("ItemCount", 0)
            for k in desc.get('KeySchema', []):
                if k['KeyType'] == 'HASH':
                    info["partition_key"] = k['AttributeName']
                elif k['KeyType'] == 'RANGE':
                    info["sort_key"] = k['AttributeName']
        except Exception:
            pass
        tables_info.append(info)

    return {
        "env": env,
        "count": len(tables_info),
        "tables": tables_info
    }


def empty_dynamodb_table(table_name, env='desa', key_name='custom_key'):
    """
    Vacía una tabla de DynamoDB escaneando y eliminando items por partition key.
    """
    dynamodb = get_dynamodb_resource(env)
    table = dynamodb.Table(table_name)
    count = 0
    scan_kwargs = {}
    while True:
        resp = table.scan(**scan_kwargs)
        items = resp.get('Items', [])
        for item in items:
            if key_name in item:
                table.delete_item(Key={key_name: item[key_name]})
                count += 1
        if 'LastEvaluatedKey' in resp:
            scan_kwargs['ExclusiveStartKey'] = resp['LastEvaluatedKey']
        else:
            break

    return {
        "status": "success",
        "table_name": table_name,
        "env": env,
        "deleted_count": count
    }
