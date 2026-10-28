#!/usr/bin/env python3
"""
cleanup_manager.py
Lógica de negocio para la gestión y eliminación de registros de bots en DynamoDB.

Definición del negocio:
- Un "registro" corresponde a un ítem (cliente / bot) en la tabla {env}_sonia_config
  (desa_sonia_config, test_sonia_config o prod_sonia_config).
- Eliminar un bot de DynamoDB consiste en ejecutar delete_item en la tabla
  {env}_sonia_config para eliminar sus prompts, configuración y existencia en dicho ambiente.
- Por seguridad, siempre se realiza un backup JSON automático previo en backups/cleanup/
  y se deja constancia en backups/cleanup_audit.jsonl.
"""

import copy
import csv
import datetime
import io
import json
import os
import pathlib
import threading
import time
from decimal import Decimal

from promote_config import ENVS, KEYS, client_for, table_for, scan_keys, get_item

BACKUPS_CLEANUP_DIR = os.path.join("backups", "cleanup")
AUDIT_LOG_FILE = os.path.join("backups", "cleanup_audit.jsonl")

_cache_lock = threading.Lock()
_records_cache = {}  # (env, client): (timestamp, data)
CACHE_TTL = 120  # segundos


def _safe_str(val):
    if val is None:
        return ""
    if isinstance(val, dict):
        if "S" in val:
            return str(val["S"])
        if "N" in val:
            return str(val["N"])
    return str(val)


def _json_default(obj):
    if isinstance(obj, Decimal):
        return float(obj) if obj % 1 else int(obj)
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    if isinstance(obj, set):
        return list(obj)
    return str(obj)


def get_bot_system_id(item, bot_name):
    """
    Determina el identificador de sistema del bot/agente.
    Por ejemplo: el nombre de la State Machine / Processor o 'agente-{bot}'.
    """
    if not item or not isinstance(item, dict):
        return f"agente-{bot_name}"

    # 1. Intentar obtener Step Function ARN
    bb = item.get("BUILDERBOT_CONFIG")
    if isinstance(bb, dict):
        bb_m = bb.get("M", bb)
        exec_cfg = bb_m.get("EXECUTION")
        if isinstance(exec_cfg, dict):
            exec_m = exec_cfg.get("M", exec_cfg)
            arn_val = exec_m.get("STEP_FUNCTION_ARN")
            arn = _safe_str(arn_val)
            if arn and "stateMachine:" in arn:
                return arn.split("stateMachine:")[-1]

    # 2. BOT_ROLE_ID si existe
    ac = item.get("AGENT_CONFIG")
    if isinstance(ac, dict):
        ac_m = ac.get("M", ac)
        role = ac_m.get("BOT_ROLE_ID")
        role_s = _safe_str(role)
        if role_s and role_s != "0":
            return f"agente-{bot_name} (rol {role_s})"

    return f"agente-{bot_name}"


def scan_single_client_records(env, client, bots_list):
    """
    Cada bot en {env}_sonia_config representa 1 registro (su existencia, configuración y prompts).
    """
    t = table_for(env)
    tbl_name = getattr(t, "name", str(t))
    bot_stats = {}
    for b in bots_list:
        bot_stats[b] = {
            "records_count": 1,
            "dynamodb_records": 1,
            "tables": {tbl_name: 1},
            "last_updated": "Configurado en DynamoDB",
            "items_to_delete": [(tbl_name, {"client": {"S": client}, "bot": {"S": b}}, None)]
        }
    return bot_stats


def get_client_records_cached(env, client, bots_list, refresh=False):
    """Consulta los registros de un cliente utilizando caché en memoria."""
    cache_key = (env, client)
    now = time.time()
    with _cache_lock:
        hit = _records_cache.get(cache_key)
        if hit and not refresh and (now - hit[0] < CACHE_TTL):
            return hit[1]

    stats = scan_single_client_records(env, client, bots_list)
    with _cache_lock:
        _records_cache[cache_key] = (now, stats)
    return stats


def clear_client_records_cache(env, client=None):
    """Invalida la caché para un cliente o para todo el ambiente."""
    with _cache_lock:
        if client:
            _records_cache.pop((env, client), None)
        else:
            to_del = [k for k in _records_cache if k[0] == env]
            for k in to_del:
                _records_cache.pop(k, None)


def get_cleanup_overview(env, refresh=False, load_records=True):
    """
    Obtiene la estructura completa de clientes y bots para la vista Cleanup.
    Cada bot en {env}_sonia_config es 1 registro en DynamoDB.
    """
    if env not in ENVS:
        raise ValueError(f"Ambiente inválido: {env}")

    t = table_for(env)
    tbl_name = getattr(t, "name", str(t))
    keys = scan_keys(t)  # lista ordenada de tuplas (client, bot)

    # Agrupar bots por cliente
    client_bots_map = {}
    for c, b in keys:
        client_bots_map.setdefault(c, []).append(b)

    clients_data = []
    total_records_env = len(keys)

    for client_name in sorted(client_bots_map.keys()):
        bots = client_bots_map[client_name]
        bot_items = []
        for b in bots:
            system_id = f"agente-{b}"
            bot_items.append({
                "bot": b,
                "system_id": system_id,
                "records_count": 1,
                "dynamodb_records": 1,
                "tables": {tbl_name: 1},
                "last_updated": "Configurado en DynamoDB",
                "has_records": True
            })

        clients_data.append({
            "client": client_name,
            "bots_count": len(bots),
            "total_records": len(bots),
            "last_updated": "Configurado en DynamoDB",
            "bots": bot_items,
            "bot_names": bots,
            "tags_preview": bots
        })

    return {
        "env": env,
        "table": tbl_name,
        "total_clients": len(clients_data),
        "total_bots": len(keys),
        "total_records": total_records_env,
        "clients": clients_data
    }


def delete_agent_records(env, targets, confirm_prod=False, operator="admin"):
    """
    Elimina de DynamoDB los bots seleccionados ejecutando delete_item sobre {env}_sonia_config.
    Borra su existencia, prompts y configuración del ambiente seleccionado.
    """
    if env not in ENVS:
        raise ValueError(f"Ambiente inválido: {env}")

    if not isinstance(targets, list) or not targets:
        raise ValueError("No se seleccionaron agentes para eliminar.")

    if env == "prod" and not confirm_prod:
        raise PermissionError("La eliminación en el ambiente PROD requiere confirmación explícita.")

    t = table_for(env)
    tbl_name = getattr(t, "name", str(t))
    valid_keys = set(scan_keys(t))

    validated_targets = []
    for tgt in targets:
        c, b = tgt.get("client"), tgt.get("bot")
        if not c or not b:
            continue
        if (c, b) not in valid_keys:
            raise ValueError(f"El bot {c}/{b} no existe en la configuración del ambiente {env.upper()}.")
        validated_targets.append((c, b))

    if not validated_targets:
        raise ValueError("Ninguno de los agentes seleccionados es válido.")

    # Crear carpeta de backups
    os.makedirs(BACKUPS_CLEANUP_DIR, exist_ok=True)
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

    c_dynamo = client_for(env)
    success_bots = []
    failed_bots = []
    total_deleted = 0
    backup_files = []

    for client_name, b in validated_targets:
        bot_errors = []

        # 1. Realizar backup del ítem antes de eliminar de DynamoDB
        try:
            raw_item = get_item(t, (client_name, b))
            if raw_item:
                backup_data = {
                    "env": env,
                    "client": client_name,
                    "bot": b,
                    "table": tbl_name,
                    "deleted_at": ts,
                    "item": raw_item
                }
                b_file = os.path.join(BACKUPS_CLEANUP_DIR, f"{env}_{client_name}_{b}_{ts}.json")
                with open(b_file, "w", encoding="utf-8") as f:
                    json.dump(backup_data, f, ensure_ascii=False, indent=2, default=_json_default)
                backup_files.append(b_file)
        except Exception as ex:
            bot_errors.append(f"Error creando backup: {ex}")

        # 2. Ejecutar DynamoDB delete_item sobre {env}_sonia_config
        try:
            if hasattr(t, "delete_item"):
                t.delete_item(Key={"client": client_name, "bot": b})
            else:
                c_dynamo.delete_item(
                    TableName=tbl_name,
                    Key={
                        "client": {"S": client_name},
                        "bot": {"S": b}
                    }
                )
            total_deleted += 1
            success_bots.append({
                "client": client_name,
                "bot": b,
                "deleted": 1,
                "tables": {tbl_name: 1}
            })
        except Exception as ex:
            bot_errors.append(f"Error al ejecutar delete_item en DynamoDB ({tbl_name}): {ex}")
            failed_bots.append({
                "client": client_name,
                "bot": b,
                "deleted": 0,
                "errors": bot_errors
            })

        clear_client_records_cache(env, client_name)

    # 3. Registrar auditoría
    audit_entry = {
        "timestamp": datetime.datetime.now().isoformat(),
        "env": env,
        "operator": operator,
        "action": "delete_item_dynamodb",
        "table": tbl_name,
        "total_deleted": total_deleted,
        "success_count": len(success_bots),
        "failed_count": len(failed_bots),
        "success_bots": [(s["client"], s["bot"]) for s in success_bots],
        "failed_bots": [(f["client"], f["bot"]) for f in failed_bots],
        "backup_files": backup_files
    }
    try:
        with open(AUDIT_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(audit_entry, ensure_ascii=False) + "\n")
    except Exception:
        pass

    return {
        "status": "success" if not failed_bots else "partial" if success_bots else "failed",
        "env": env,
        "table": tbl_name,
        "total_deleted": total_deleted,
        "total_deleted_records": total_deleted,
        "processed_bots": len(success_bots),
        "success_bots": success_bots,
        "failed_bots": failed_bots,
        "backup_files": backup_files,
        "backup_file": backup_files[0] if backup_files else None
    }


def export_selection_csv(env, targets):
    """
    Genera un archivo CSV con la selección actual.
    Incluye BOM UTF-8 para apertura correcta en Microsoft Excel.
    """
    if env not in ENVS:
        raise ValueError(f"Ambiente inválido: {env}")

    overview = get_cleanup_overview(env, refresh=False, load_records=True)

    # Crear un índice de los datos
    indexed = {}
    for cl in overview["clients"]:
        for b in cl["bots"]:
            indexed[(cl["client"], b["bot"])] = {
                "system_id": b["system_id"],
                "records_count": b["records_count"],
                "tables": b["tables"],
                "last_updated": b["last_updated"] or "—"
            }

    query_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    output = io.StringIO()
    writer = csv.writer(output, delimiter=",", quoting=csv.QUOTE_MINIMAL)

    # Cabecera
    writer.writerow([
        "Ambiente",
        "Cliente",
        "Bot / Agente",
        "Identificador",
        "Registros DynamoDB",
        "Detalle Tablas",
        "Última Actualización",
        "Fecha de Consulta"
    ])

    for tgt in targets:
        c = tgt.get("client")
        b = tgt.get("bot")
        info = indexed.get((c, b), {
            "system_id": f"agente-{b}",
            "records_count": 1,
            "tables": {f"{env}_sonia_config": 1},
            "last_updated": "Configurado en DynamoDB"
        })

        tables_desc = "; ".join(f"{t}: {cnt}" for t, cnt in info["tables"].items()) if info["tables"] else "Configurado"

        writer.writerow([
            env.upper(),
            c,
            b,
            info["system_id"],
            info["records_count"],
            tables_desc,
            info["last_updated"],
            query_date
        ])

    csv_content = output.getvalue()
    return "\ufeff" + csv_content
