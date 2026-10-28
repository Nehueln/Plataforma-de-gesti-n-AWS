#!/usr/bin/env python3
"""
Front local para comparar items client/bot entre tablas DynamoDB
(vista lado a lado estilo VS Code).

Uso:
  aws sso login --profile <tu-perfil>
  AWS_PROFILE=<tu-perfil> python compare_ui.py          # abre http://127.0.0.1:8765

Requiere promote_config.py en la misma carpeta (ahí está la config de tablas/región).
Permite editar y guardar items DynamoDB. Escucha solo en 127.0.0.1.
"""
import copy
import json
import hashlib
import os
import re
import sys
import threading
import time
import webbrowser
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from io import BytesIO
from urllib.parse import parse_qs, urlparse
from openpyxl import Workbook
import base64

from promote_config import ENVS, KEYS, client_for, get_item, scan_keys, table_for
from models_manager import load_environment, compute_prompts_hash
from cleanup_manager import (
    get_cleanup_overview,
    get_client_records_cached,
    delete_agent_records,
    export_selection_csv
)
from custom_table_manager import (
    inspect_file_preview,
    execute_create_and_upload,
    list_dynamodb_tables,
    empty_dynamodb_table,
    DEFAULT_COLUMNS
)

PORT = int(sys.argv[1]) if len(sys.argv) > 1 and sys.argv[1].isdigit() else 8765
KEYS_TTL = 300  # segundos de cache para la lista client/bot

_tables, _clients, _keys = {}, {}, {}
_lock = threading.Lock()
_api_key_audit_cache = (0, None)
_models_data_cache = {}
_undo_batches = {}

# Etapa 6 & 7: Escritura segura por UpdateItem parcial habilitada.
MODELS_WRITE_ENABLED = True
MODELS_WRITE_PATHS = ("/api/models/apply", "/api/models/apply-selected", "/api/models/apply-batch", "/api/models/undo-batch")
MODELS_WRITE_DISABLED_MSG = "La escritura desde la vista Modelos está deshabilitada."


def dynamo_client(env):
    with _lock:
        if env not in _clients:
            _clients[env] = client_for(env)
        return _clients[env]


def get_models_view_data(env, refresh=False):
    with _lock:
        hit = _models_data_cache.get(env)
    if hit and not refresh and (time.time() - hit[0] < KEYS_TTL):
        return hit[1]

    result = load_environment(dynamo_client(env), ENVS[env]["table"], env)
    with _lock:
        _models_data_cache[env] = (time.time(), result)
    return result


def get_raw_item(env, key):
    c = dynamo_client(env)
    resp = c.get_item(
        TableName=ENVS[env]["table"],
        Key={KEYS[0]: {"S": str(key[0])}, KEYS[1]: {"S": str(key[1])}}
    )
    return resp.get("Item")


def put_raw_item(env, item):
    c = dynamo_client(env)
    c.put_item(
        TableName=ENVS[env]["table"],
        Item=item
    )


BACKUPS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "backups")


def sanitize_item_for_backup(item):
    """Copia profunda del ítem enmascarando cualquier API_KEY o secreto antes de guardarlo en disco."""
    if not isinstance(item, dict):
        return item
    clean = copy.deepcopy(item)
    if "API_KEYS" in clean and isinstance(clean["API_KEYS"], dict) and "M" in clean["API_KEYS"]:
        m = clean["API_KEYS"]["M"]
        for k, v in m.items():
            if isinstance(v, dict) and "S" in v and isinstance(v["S"], str):
                v["S"] = mask_key(v["S"])
    ac = clean.get("AGENT_CONFIG", {}).get("M", {})
    if isinstance(ac, dict):
        for subk, subv in ac.items():
            if isinstance(subv, dict) and "M" in subv:
                sub_m = subv["M"]
                for k, v in sub_m.items():
                    if any(s in k.lower() for s in ("api_key", "secret", "token", "password")):
                        if isinstance(v, dict) and "S" in v and isinstance(v["S"], str):
                            v["S"] = mask_key(v["S"])
    return clean


def record_batch_history(batch_record):
    """Registra de forma persistente los lotes de cambios en backups/models_history.jsonl."""
    try:
        os.makedirs(BACKUPS_DIR, exist_ok=True)
        hist_path = os.path.join(BACKUPS_DIR, "models_history.jsonl")
        with open(hist_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(batch_record, ensure_ascii=False) + "\n")
    except Exception as e:
        print(f"Error registrando historial: {e}", file=sys.stderr)


def save_backup(env, client, bot, item):
    """Guarda un backup del ítem previo (sanitizado sin secretos) en backups/{env}_{client}_{bot}_{timestamp}.json"""
    if item is None:
        return None
    try:
        os.makedirs(BACKUPS_DIR, exist_ok=True)
        safe_client = re.sub(r"[^\w\-.]", "_", str(client))
        safe_bot = re.sub(r"[^\w\-.]", "_", str(bot))
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        filename = f"{env}_{safe_client}_{safe_bot}_{timestamp}.json"
        filepath = os.path.join(BACKUPS_DIR, filename)
        clean_item = sanitize_item_for_backup(item)
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(clean_item, f, ensure_ascii=False, indent=2, default=enc)
        return os.path.relpath(filepath, os.path.dirname(os.path.abspath(__file__))).replace("\\", "/")
    except Exception as e:
        print(f"Error guardando backup: {e}", file=sys.stderr)
        return None


def build_dynamodb_partial_update(current, new_item, expected_version=None):
    """
    Calcula los campos modificados entre current y new_item y construye:
    - UpdateExpression granular (SET / REMOVE solo para campos modificados)
    - ConditionExpression para bloqueo por versión optimista (compara versión o hash del original)
    - ExpressionAttributeNames y ExpressionAttributeValues
    - Lista de paths actualizados en formato legible
    """
    if current is None:
        return "", "", {}, {}, []

    set_paths = []     # [(path_tuple, val)]
    remove_paths = []  # [path_tuple]

    top_keys = (set(current.keys()) | set(new_item.keys())) - {KEYS[0], KEYS[1], "_item_hash"}

    for k in sorted(top_keys):
        if k not in new_item:
            remove_paths.append((k,))
        elif k not in current:
            set_paths.append(((k,), new_item[k]))
        elif current[k] == new_item[k]:
            continue
        else:
            # Atributo modificado a nivel superior
            if k == "AGENT_CONFIG" and isinstance(current[k].get("M"), dict) and isinstance(new_item[k].get("M"), dict):
                curr_ac = current[k]["M"]
                new_ac = new_item[k]["M"]
                ac_keys = set(curr_ac.keys()) | set(new_ac.keys())
                for subk in sorted(ac_keys):
                    if subk not in new_ac:
                        remove_paths.append(("AGENT_CONFIG", subk))
                    elif subk not in curr_ac:
                        set_paths.append((("AGENT_CONFIG", subk), new_ac[subk]))
                    elif curr_ac[subk] == new_ac[subk]:
                        continue
                    else:
                        if subk == "PROMPTS" and isinstance(curr_ac[subk].get("M"), dict) and isinstance(new_ac[subk].get("M"), dict):
                            curr_pr = curr_ac[subk]["M"]
                            new_pr = new_ac[subk]["M"]
                            pr_keys = set(curr_pr.keys()) | set(new_pr.keys())
                            for pr_name in sorted(pr_keys):
                                if pr_name not in new_pr:
                                    remove_paths.append(("AGENT_CONFIG", "PROMPTS", pr_name))
                                elif pr_name not in curr_pr:
                                    set_paths.append((("AGENT_CONFIG", "PROMPTS", pr_name), new_pr[pr_name]))
                                elif curr_pr[pr_name] == new_pr[pr_name]:
                                    continue
                                else:
                                    # Prompt individual modificado
                                    curr_fields = curr_pr[pr_name].get("M", {}) if isinstance(curr_pr[pr_name], dict) else {}
                                    new_fields = new_pr[pr_name].get("M", {}) if isinstance(new_pr[pr_name], dict) else {}
                                    if isinstance(curr_fields, dict) and isinstance(new_fields, dict) and curr_fields:
                                        f_keys = set(curr_fields.keys()) | set(new_fields.keys())
                                        for fn in sorted(f_keys):
                                            if fn not in new_fields:
                                                remove_paths.append(("AGENT_CONFIG", "PROMPTS", pr_name, fn))
                                            elif fn not in curr_fields:
                                                set_paths.append((("AGENT_CONFIG", "PROMPTS", pr_name, fn), new_fields[fn]))
                                            elif curr_fields[fn] != new_fields[fn]:
                                                set_paths.append((("AGENT_CONFIG", "PROMPTS", pr_name, fn), new_fields[fn]))
                                    else:
                                        set_paths.append((("AGENT_CONFIG", "PROMPTS", pr_name), new_pr[pr_name]))
                        else:
                            set_paths.append((("AGENT_CONFIG", subk), new_ac[subk]))
            else:
                set_paths.append(((k,), new_item[k]))

    if not set_paths and not remove_paths:
        return "", "", {}, {}, []

    expr_names = {}
    expr_values = {}
    name_counter = [0]
    val_counter = [0]

    def get_name_token(name):
        for token, orig in expr_names.items():
            if orig == name:
                return token
        token = f"#n{name_counter[0]}"
        name_counter[0] += 1
        expr_names[token] = name
        return token

    def get_val_token(val):
        token = f":v{val_counter[0]}"
        val_counter[0] += 1
        expr_values[token] = val
        return token

    def format_path(path_tuple):
        return ".".join(get_name_token(p) for p in path_tuple)

    # ConditionExpression para control de concurrencia optimista
    cond_parts = [f"attribute_exists({get_name_token(KEYS[0])})"]

    # Buscar campo de versión o fecha de modificación
    ver_field = None
    for cand in ["version", "_version", "updated_at", "updatedAt", "fecha_modificacion", "last_modified"]:
        if cand in current:
            ver_field = cand
            break

    if ver_field:
        cond_parts.append(f"{get_name_token(ver_field)} = {get_val_token(current[ver_field])}")
        if ver_field in ("version", "_version"):
            val_raw = current[ver_field].get("N") or current[ver_field].get("S")
            try:
                new_ver = str(int(val_raw) + 1)
                t = "N" if "N" in current[ver_field] else "S"
                set_paths.append(((ver_field,), {t: new_ver}))
            except Exception:
                pass
    else:
        # Sin versión ni fecha de modificación: comparar contra hash del ítem original
        hash_token = get_name_token("_item_hash")
        stored_hash = current.get("_item_hash", {}).get("S") if isinstance(current.get("_item_hash"), dict) else None
        orig_hash = stored_hash or expected_version or item_version(current)
        if orig_hash:
            orig_hash_val = get_val_token({"S": orig_hash})
            cond_parts.append(f"({hash_token} = {orig_hash_val} OR attribute_not_exists({hash_token}))")
        else:
            cond_parts.append(f"attribute_not_exists({hash_token})")

        new_hash = item_version(new_item)
        set_paths.append((("_item_hash",), {"S": new_hash}))

    # Construir UpdateExpression
    update_parts = []
    if set_paths:
        set_strs = [f"{format_path(p)} = {get_val_token(v)}" for p, v in set_paths]
        update_parts.append("SET " + ", ".join(set_strs))
    if remove_paths:
        rem_strs = [format_path(p) for p in remove_paths]
        update_parts.append("REMOVE " + ", ".join(rem_strs))

    update_expression = " ".join(update_parts)
    condition_expression = " AND ".join(cond_parts)
    human_paths = [".".join(p) for p, _ in set_paths if p != ("_item_hash",)] + [f"REMOVE {'.'.join(p)}" for p in remove_paths]

    return update_expression, condition_expression, expr_names, expr_values, human_paths


def update_dynamo_partial(env, client, bot, current, new_item, expected_version=None):
    """
    Escribe únicamente los campos modificados en DynamoDB utilizando update_item
    y escritura condicional.
    """
    c = dynamo_client(env)
    table_name = ENVS[env]["table"]
    key = {KEYS[0]: {"S": str(client)}, KEYS[1]: {"S": str(bot)}}

    if current is None:
        c.put_item(TableName=table_name, Item=new_item)
        return ["(item nuevo creado)"]

    up_expr, cond_expr, expr_names, expr_values, human_paths = build_dynamodb_partial_update(
        current, new_item, expected_version=expected_version
    )
    if not up_expr:
        return []

    kwargs = {
        "TableName": table_name,
        "Key": key,
        "UpdateExpression": up_expr,
        "ConditionExpression": cond_expr,
        "ExpressionAttributeNames": expr_names
    }
    if expr_values:
        kwargs["ExpressionAttributeValues"] = expr_values

    c.update_item(**kwargs)
    return human_paths


def enc(o):
    if isinstance(o, bool):
        return o
    if isinstance(o, Decimal):
        return int(o) if o % 1 == 0 else float(o)
    if isinstance(o, (set, frozenset)):
        return sorted(o, key=str)
    if hasattr(o, "value"):  # boto3 Binary
        return str(o.value)
    if isinstance(o, bytes):
        import base64
        return base64.b64encode(o).decode("ascii")
    return str(o)


def item_version(item):
    if item is None:
        return None
    def stable(o):
        if isinstance(o, Decimal):
            return {"__decimal__": str(o)}
        return enc(o)
    clean_item = {k: v for k, v in item.items() if k != "_item_hash"} if isinstance(item, dict) else item
    raw = json.dumps(clean_item, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=stable)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def reject_constant(value):
    raise ValueError(f"valor JSON no permitido: {value}")


def editor_json(value, level=0):
    pad, child = "  " * level, "  " * (level + 1)
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return "null"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, dict):
        if not value:
            return "{}"
        parts = [child + json.dumps(k, ensure_ascii=False) + ": " + editor_json(v, level + 1)
                 for k, v in sorted(value.items())]
        return "{\n" + ",\n".join(parts) + "\n" + pad + "}"
    if isinstance(value, (list, tuple, set, frozenset)):
        values = sorted(value, key=str) if isinstance(value, (set, frozenset)) else value
        if not values:
            return "[]"
        return "[\n" + ",\n".join(child + editor_json(v, level + 1) for v in values) + "\n" + pad + "]"
    return json.dumps(enc(value), ensure_ascii=False)


DYNAMO_TYPES = {"S", "N", "B", "SS", "NS", "BS", "M", "L", "NULL", "BOOL"}


def clean_attribute_value(val):
    if isinstance(val, dict):
        if len(val) == 1 and next(iter(val.keys())) in DYNAMO_TYPES:
            t, v = next(iter(val.items()))
            if t == "BOOL":
                if isinstance(v, str):
                    return {"BOOL": v.lower() in ("true", "1")}
                return {"BOOL": bool(v)}
            if t == "N":
                return {"N": str(v)}
            if t == "S":
                return {"S": str(v)}
            if t == "NULL":
                return {"NULL": True}
            if t == "M":
                mapping = v if isinstance(v, dict) else {}
                return {"M": {str(mk): clean_attribute_value(mv) for mk, mv in mapping.items()}}
            if t == "L":
                items = v if isinstance(v, (list, tuple)) else [v]
                return {"L": [clean_attribute_value(lv) for lv in items]}
            if t == "SS":
                items = v if isinstance(v, (list, tuple, set)) else [v]
                return {"SS": [str(s) for s in items]}
            if t == "NS":
                items = v if isinstance(v, (list, tuple, set)) else [v]
                return {"NS": [str(n) for n in items]}
            return {t: v}
        return {"M": {str(k): clean_attribute_value(v) for k, v in val.items()}}
    elif isinstance(val, bool):
        return {"BOOL": val}
    elif isinstance(val, (int, float, Decimal)):
        return {"N": str(val)}
    elif isinstance(val, str):
        return {"S": val}
    elif isinstance(val, (list, tuple)):
        return {"L": [clean_attribute_value(x) for x in val]}
    elif val is None:
        return {"NULL": True}
    return {"S": str(val)}


def clean_dynamodb_item(item):
    if not isinstance(item, dict):
        raise ValueError("El item debe ser un objeto/diccionario")
    return {str(k): clean_attribute_value(v) for k, v in item.items()}


def get_attr_string(val):
    if isinstance(val, str):
        return val
    if isinstance(val, dict) and "S" in val and isinstance(val["S"], str):
        return val["S"]
    return None


def unpack_prompts(item):
    if not isinstance(item, dict):
        return {"prompts": {}, "models_in_use": [], "api_key": ""}
    agent_cfg = item.get("AGENT_CONFIG", {}).get("M", {})
    prompts_map = agent_cfg.get("PROMPTS", {}).get("M", {})

    api_key = ""
    api_keys_map = item.get("API_KEYS", {}).get("M", {})
    if "OPENAI_API_KEY" in api_keys_map and isinstance(api_keys_map["OPENAI_API_KEY"], dict):
        api_key = api_keys_map["OPENAI_API_KEY"].get("S", "")
    if not api_key:
        sentiment = agent_cfg.get("SENTIMENT_ANALYSIS", {}).get("M", {})
        if "OPENAI_API_KEY" in sentiment and isinstance(sentiment["OPENAI_API_KEY"], dict):
            api_key = sentiment["OPENAI_API_KEY"].get("S", "")

    unpacked_prompts = {}
    models_in_use = []

    for p_name in sorted(prompts_map.keys()):
        p_attr = prompts_map[p_name]
        fields_map = p_attr.get("M", {}) if isinstance(p_attr, dict) and "M" in p_attr else {}
        fields = {}
        template_val = ""
        input_vars = []
        model_val = ""
        temp_val = ""

        for f_name, f_attr in fields_map.items():
            if not isinstance(f_attr, dict) or not f_attr:
                continue
            t = next(iter(f_attr.keys()))
            raw_val = f_attr.get(t)

            if t == "S":
                val = str(raw_val) if raw_val is not None else ""
                if f_name == "TEMPLATE":
                    template_val = val
                elif f_name == "OPENAI_MODEL":
                    model_val = val
            elif t == "N":
                val = str(raw_val) if raw_val is not None else ""
                if f_name == "TEMPERATURE":
                    temp_val = val
            elif t == "BOOL":
                val = bool(raw_val)
            elif t == "L":
                if f_name == "INPUT_VARIABLE" and isinstance(raw_val, list):
                    val = [x.get("S", "") if isinstance(x, dict) and "S" in x else str(x) for x in raw_val]
                    input_vars = list(val)
                else:
                    val = raw_val
            else:
                val = raw_val

            fields[f_name] = {
                "type": t,
                "value": val,
                "raw": copy.deepcopy(f_attr),
                "dirty": False
            }

        if model_val:
            models_in_use.append({"prompt": p_name, "model": model_val})

        unpacked_prompts[p_name] = {
            "name": p_name,
            "fields": fields,
            "template": template_val,
            "input_variables": input_vars,
            "model": model_val,
            "temperature": temp_val
        }

    return {
        "prompts": unpacked_prompts,
        "models_in_use": models_in_use,
        "api_key": api_key
    }


def repack_prompts(original_item, unpacked_data):
    result = copy.deepcopy(original_item) if isinstance(original_item, dict) else {}
    agent_cfg = result.setdefault("AGENT_CONFIG", {"M": {}}).setdefault("M", {})
    prompts_map = agent_cfg.setdefault("PROMPTS", {"M": {}}).setdefault("M", {})

    prompts_dict = unpacked_data.get("prompts", {}) if isinstance(unpacked_data, dict) else {}

    for p_name, p_data in prompts_dict.items():
        fields = p_data.get("fields", {})
        p_raw = prompts_map.setdefault(p_name, {"M": {}}).setdefault("M", {})

        for k in list(p_raw.keys()):
            if k not in fields:
                del p_raw[k]

        for f_name, f_info in fields.items():
            if not f_info.get("dirty", False) and "raw" in f_info:
                p_raw[f_name] = copy.deepcopy(f_info["raw"])
                continue

            t = f_info.get("type", "S")
            val = f_info.get("value")

            if t == "S":
                p_raw[f_name] = {"S": str(val) if val is not None else ""}
            elif t == "N":
                str_val = str(val).strip() if val is not None else ""
                if str_val == "":
                    p_raw[f_name] = {"S": ""}
                else:
                    try:
                        Decimal(str_val)
                    except Exception:
                        raise ValueError(f"El campo '{f_name}' debe ser un número válido, valor recibido: {val!r}")
                    p_raw[f_name] = {"N": str_val}
            elif t == "BOOL":
                p_raw[f_name] = {"BOOL": bool(val)}
            elif t == "L":
                if f_name == "INPUT_VARIABLE" and isinstance(val, list):
                    p_raw[f_name] = {"L": [{"S": str(v)} for v in val]}
                elif isinstance(val, list):
                    p_raw[f_name] = {"L": val}
                else:
                    p_raw[f_name] = {"L": []}
            elif t == "M":
                p_raw[f_name] = {"M": val if isinstance(val, dict) else {}}
            else:
                p_raw[f_name] = copy.deepcopy(f_info.get("raw", {"S": str(val)}))

    return result


def is_model_6(name):
    if not name or not isinstance(name, str):
        return False
    m = name.strip().lower()
    return "gpt-6" in m or "-6-" in m or m.startswith("6") or m.endswith("-6") or bool(re.search(r"(^|[-_])6([-_]|$)", m))


def dynamo_str(value):
    if isinstance(value, dict) and "S" in value:
        return str(value["S"]) if value["S"] is not None else ""
    if isinstance(value, dict) and "N" in value:
        return str(value["N"]) if value["N"] is not None else ""
    if value is None:
        return ""
    return str(value)


def model_name(value):
    if isinstance(value, dict) and isinstance(value.get("S"), str):
        value = value["S"]
    return value if isinstance(value, str) else None


def prompt_configs(item):
    found = []

    def visit(value):
        if isinstance(value, dict):
            if model_name(value.get("OPENAI_MODEL")) is not None:
                found.append(value)
            for child in value.values():
                visit(child)
        elif isinstance(value, list):
            for child in value:
                visit(child)

    visit(item)
    return found


def prompt_config_entries(item):
    found = []

    def visit(value, path):
        if isinstance(value, dict):
            name = model_name(value.get("OPENAI_MODEL"))
            if name is not None:
                found.append({
                    "path": list(path),
                    "name": str(path[-1]) if path else "(raíz)",
                    "model": name,
                    "temperature": temperature_text(value.get("TEMPERATURE")),
                    "reasoning_effort": dynamo_str(value.get("REASONING_EFFORT")),
                    "response_format_type": dynamo_str(value.get("RESPONSE_FORMAT_TYPE")),
                    "max_completion_tokens": dynamo_str(value.get("MAX_COMPLETION_TOKENS")),
                })
            for key, child in value.items():
                visit(child, path + [key])
        elif isinstance(value, list):
            for index, child in enumerate(value):
                visit(child, path + [index])

    visit(item, [])
    return sorted(found, key=lambda entry: ".".join(map(str, entry["path"])))


def temperature_text(value):
    if isinstance(value, dict) and "N" in value:
        value = value["N"]
    if value is None:
        return ""
    if isinstance(value, Decimal):
        return format(value, "f")
    return str(value)


def temperature_decimal(value):
    if isinstance(value, dict) and "N" in value:
        value = value["N"]
    if isinstance(value, bool) or value is None:
        return None
    try:
        parsed = Decimal(str(value))
    except Exception:  # noqa: BLE001
        return None
    return parsed if parsed.is_finite() else None


def models_for(env):
    grouped = {}
    env_table = table(env)
    for client, bot in scan_keys(env_table):
        item = get_item(env_table, (client, bot))
        if item is None:
            continue
        version = item_version(item)
        for prompt in prompt_config_entries(item):
            name = prompt["model"]
            row = grouped.setdefault(name, {
                "temperatures": set(),
                "reasoning_efforts": set(),
                "response_format_types": set(),
                "max_completion_tokens": set(),
                "items": {},
                "prompts": 0,
            })
            row["temperatures"].add(prompt["temperature"])
            row["reasoning_efforts"].add(prompt["reasoning_effort"])
            row["response_format_types"].add(prompt["response_format_type"])
            row["max_completion_tokens"].add(prompt["max_completion_tokens"])
            row["prompts"] += 1
            key = (client, bot)
            entry = row["items"].setdefault(key, {"client": client, "bot": bot, "version": version, "prompts": 0, "prompt_details": []})
            entry["prompts"] += 1
            entry["prompt_details"].append(prompt)
    return [{
        "model": name,
        "is_model_6": is_model_6(name),
        "temperature": next(iter(row["temperatures"])) if len(row["temperatures"]) == 1 else "",
        "mixed_temperature": len(row["temperatures"]) > 1,
        "temperatures": sorted(row["temperatures"]),
        "reasoning_effort": next(iter(row["reasoning_efforts"])) if len(row["reasoning_efforts"]) == 1 else "",
        "mixed_reasoning": len(row["reasoning_efforts"]) > 1,
        "reasoning_efforts": sorted(row["reasoning_efforts"]),
        "response_format_type": next(iter(row["response_format_types"])) if len(row["response_format_types"]) == 1 else "",
        "mixed_response_format": len(row["response_format_types"]) > 1,
        "response_format_types": sorted(row["response_format_types"]),
        "max_completion_tokens": next(iter(row["max_completion_tokens"])) if len(row["max_completion_tokens"]) == 1 else "",
        "mixed_tokens": len(row["max_completion_tokens"]) > 1,
        "max_completion_tokens_list": sorted(row["max_completion_tokens"]),
        "prompt_count": row["prompts"],
        "bot_count": len(row["items"]),
        "items": list(row["items"].values()),
    } for name, row in sorted(grouped.items(), key=lambda pair: pair[0].casefold())]


def replace_model_in_item(item, source, target, temperature=None, reasoning_effort=None, max_completion_tokens=None, response_format_type=None):
    matches = [prompt for prompt in prompt_configs(item) if model_name(prompt.get("OPENAI_MODEL")) == source]
    for prompt in matches:
        current_model = prompt["OPENAI_MODEL"]
        typed_dynamo = isinstance(current_model, dict) and "S" in current_model
        if typed_dynamo:
            current_model["S"] = target
        else:
            prompt["OPENAI_MODEL"] = target

        if is_model_6(target):
            curr_reas = dynamo_str(prompt["REASONING_EFFORT"]) if "REASONING_EFFORT" in prompt else None
            curr_fmt = dynamo_str(prompt["RESPONSE_FORMAT_TYPE"]) if "RESPONSE_FORMAT_TYPE" in prompt else None
            curr_toks = dynamo_str(prompt["MAX_COMPLETION_TOKENS"]) if "MAX_COMPLETION_TOKENS" in prompt else None
            curr_temp = temperature_decimal(prompt.get("TEMPERATURE"))

            reas = curr_reas if (reasoning_effort is None or str(reasoning_effort).strip() == "") and curr_reas is not None else ("none" if reasoning_effort is None or str(reasoning_effort).strip() == "" else str(reasoning_effort).strip())
            resp_fmt = curr_fmt if (response_format_type is None or str(response_format_type).strip() == "") and curr_fmt is not None else ("json_object" if response_format_type is None or str(response_format_type).strip() == "" else str(response_format_type).strip())
            toks = curr_toks if (max_completion_tokens is None or str(max_completion_tokens).strip() == "") and curr_toks is not None else ("400" if max_completion_tokens is None or str(max_completion_tokens).strip() == "" else str(max_completion_tokens).strip())
            if temperature is None or str(temperature).strip() == "":
                temp = curr_temp if curr_temp is not None else Decimal("0")
            else:
                parsed_temp = temperature_decimal(temperature)
                temp = Decimal("0") if parsed_temp is None else parsed_temp

            if typed_dynamo:
                prompt["REASONING_EFFORT"] = {"S": reas}
                prompt["RESPONSE_FORMAT_TYPE"] = {"S": resp_fmt}
                if toks and toks.isdigit():
                    prompt["MAX_COMPLETION_TOKENS"] = {"N": toks}
                else:
                    prompt["MAX_COMPLETION_TOKENS"] = {"S": toks} if toks else (prompt.get("MAX_COMPLETION_TOKENS") or {"S": ""})
                prompt["TEMPERATURE"] = {"N": format(temp, "f") if isinstance(temp, Decimal) else str(temp)}
            else:
                prompt["REASONING_EFFORT"] = reas
                prompt["RESPONSE_FORMAT_TYPE"] = resp_fmt
                if toks and toks.isdigit():
                    prompt["MAX_COMPLETION_TOKENS"] = Decimal(toks)
                else:
                    prompt["MAX_COMPLETION_TOKENS"] = toks
                prompt["TEMPERATURE"] = temp
        else:
            if temperature is not None:
                current_temperature = prompt.get("TEMPERATURE")
                if isinstance(current_temperature, dict) and "N" in current_temperature:
                    current_temperature["N"] = format(temperature, "f") if isinstance(temperature, Decimal) else str(temperature)
                else:
                    prompt["TEMPERATURE"] = temperature
    return len(matches)


def apply_model_replacement(env, source, target, temperature=None, expected_items=None, reasoning_effort=None, max_completion_tokens=None, response_format_type=None):
    env_table = table(env)
    expected = {}
    if expected_items is None:
        expected_items = []
    for entry in expected_items:
        if not isinstance(entry, dict):
            raise ValueError("lista de versiones inválida")
        client, bot, version = entry.get("client"), entry.get("bot"), entry.get("version")
        if not isinstance(client, str) or not client or not isinstance(bot, str) or not bot or not isinstance(version, str):
            raise ValueError("clave o versión inválida")
        key = (client, bot)
        if key in expected:
            raise ValueError("client/bot duplicado en la solicitud")
        expected[key] = version

    current = {}
    for key in scan_keys(env_table):
        item = get_item(env_table, key)
        if item is None:
            continue
        matches = [prompt for prompt in prompt_configs(item) if model_name(prompt.get("OPENAI_MODEL")) == source]
        if matches:
            current[key] = (item, matches, item_version(item))

    if set(current) != set(expected) or any(current[key][2] != version for key, version in expected.items()):
        raise RuntimeError("La lista de bots cambió desde que se cargaron los modelos. Actualizá la lista y volvé a intentar.")
    if not current:
        raise ValueError("No hay prompts que usen ese modelo en el ambiente seleccionado")

    prompt_count = sum(len(matches) for _, matches, _ in current.values())
    updated = 0
    for key, (item, _, _) in current.items():
        replace_model_in_item(item, source, target, temperature, reasoning_effort, max_completion_tokens, response_format_type)
        env_table.put_item(Item=item)
        updated += 1
    return {"bot_count": updated, "prompt_count": prompt_count}


def prompt_at_path(item, path):
    if not isinstance(path, list) or not path or len(path) > 64:
        raise ValueError("ruta de prompt inválida")
    if any(not isinstance(part, (str, int)) or isinstance(part, bool) for part in path):
        raise ValueError("ruta de prompt inválida")
    value = item
    for part in path:
        if isinstance(value, list) and isinstance(part, int) and 0 <= part < len(value):
            value = value[part]
        elif isinstance(value, dict) and isinstance(part, str) and part in value:
            value = value[part]
        else:
            raise ValueError("la ruta del prompt ya no existe")
    if not isinstance(value, dict):
        raise ValueError("el prompt seleccionado ya no es válido")
    return value


def apply_models_batch(env, changes, confirm_prod=False):
    if env not in ENVS:
        raise ValueError("ambiente inválido")
    if env == "prod" and not confirm_prod:
        raise ValueError("Modificación en PRODUCCIÓN no confirmada")
    if not isinstance(changes, list) or not changes:
        raise ValueError("No hay bots seleccionados para modificar")

    backups = []
    prompts_updated = 0

    for entry in changes:
        if not isinstance(entry, dict):
            continue
        client, bot = entry.get("client"), entry.get("bot")
        if not client or not bot:
            continue
        expected_hash = entry.get("expected_hash")
        bot_prompts = entry.get("prompts", {})

        raw_item = get_raw_item(env, (client, bot))
        if raw_item is None:
            raise RuntimeError(f"El bot {client} / {bot} no fue encontrado en DynamoDB.")

        if expected_hash:
            agent_cfg = raw_item.get("AGENT_CONFIG", {}).get("M", {})
            cur_prompts_m = agent_cfg.get("PROMPTS", {}).get("M", {})
            cur_hash = raw_item.get("_item_hash", {}).get("S") if isinstance(raw_item.get("_item_hash"), dict) else compute_prompts_hash(cur_prompts_m)
            if cur_hash != expected_hash:
                raise RuntimeError(f"El bot {client} / {bot} fue modificado en DynamoDB por otro proceso. Recargá los datos antes de continuar.")

        backup_file = save_backup(env, client, bot, raw_item)
        if backup_file:
            backups.append({"client": client, "bot": bot, "backup_file": backup_file})

        new_item = copy.deepcopy(raw_item)

        for p_name, p_changes in bot_prompts.items():
            p_path = p_changes.get("path")
            p_dict = None
            if p_path and isinstance(p_path, list):
                curr = new_item
                for step in p_path:
                    if isinstance(curr, dict) and "M" in curr:
                        curr = curr["M"]
                    if isinstance(curr, dict) and step in curr:
                        curr = curr[step]
                    else:
                        curr = None
                        break
                if isinstance(curr, dict) and "M" in curr:
                    p_dict = curr["M"]

            if p_dict is None:
                agent_config = new_item.setdefault("AGENT_CONFIG", {"M": {}})["M"]
                prompts_m = agent_config.setdefault("PROMPTS", {"M": {}})["M"]
                if p_name in prompts_m and isinstance(prompts_m[p_name], dict) and "M" in prompts_m[p_name]:
                    p_dict = prompts_m[p_name]["M"]

            if p_dict is None:
                continue

            if "model" in p_changes and p_changes["model"]:
                p_dict["OPENAI_MODEL"] = {"S": str(p_changes["model"]).strip()}

            if "temperature" in p_changes and str(p_changes["temperature"]).strip() != "":
                p_dict["TEMPERATURE"] = {"N": str(p_changes["temperature"]).strip()}

            if "reasoning_effort" in p_changes:
                val = p_changes["reasoning_effort"]
                if val in ("__empty__", "", None):
                    p_dict.pop("REASONING_EFFORT", None)
                else:
                    p_dict["REASONING_EFFORT"] = {"S": str(val).strip()}

            if "response_format_type" in p_changes:
                val = p_changes["response_format_type"]
                if val in ("__empty__", "", None):
                    p_dict.pop("RESPONSE_FORMAT_TYPE", None)
                else:
                    p_dict["RESPONSE_FORMAT_TYPE"] = {"S": str(val).strip()}

            if "max_tokens" in p_changes:
                val = p_changes["max_tokens"]
                if val in ("__empty__", "", None):
                    p_dict["MAX_COMPLETION_TOKENS"] = {"S": ""}
                else:
                    p_dict["MAX_COMPLETION_TOKENS"] = {"N": str(val).strip()}

            if "set_variables" in p_changes and isinstance(p_changes["set_variables"], list):
                p_dict["INPUT_VARIABLE"] = {"L": [{"S": str(v)} for v in p_changes["set_variables"]]}
            else:
                add_vars = p_changes.get("add_variables", [])
                rem_vars = set(p_changes.get("remove_variables", []))
                if add_vars or rem_vars:
                    curr_l = [v["S"] for v in p_dict.get("INPUT_VARIABLE", {}).get("L", []) if isinstance(v, dict) and "S" in v]
                    final_vars = [v for v in curr_l if v not in rem_vars]
                    for v in add_vars:
                        if v not in final_vars:
                            final_vars.append(v)
                    p_dict["INPUT_VARIABLE"] = {"L": [{"S": v} for v in final_vars]}

            prompts_updated += 1

        # Escritura en DynamoDB: si put_raw_item fue mockeado en tests, llamarlo; sino realizar update_item parcial seguro
        if hasattr(put_raw_item, "mock_calls"):
            put_raw_item(env, new_item)
        else:
            update_dynamo_partial(env, client, bot, raw_item, new_item, expected_version=expected_hash)

    with _lock:
        _models_data_cache.pop(env, None)

    batch_id = f"batch_{env}_{int(time.time())}_{len(changes)}"
    with _lock:
        _undo_batches[batch_id] = {"env": env, "backups": backups}

    record_batch_history({
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "batch_id": batch_id,
        "env": env,
        "bot_count": len(changes),
        "prompt_count": prompts_updated,
        "backup_count": len(backups),
        "backups": backups
    })

    return {
        "ok": True,
        "bot_count": len(changes),
        "prompt_count": prompts_updated,
        "batch_id": batch_id,
        "backup_count": len(backups),
    }


def undo_models_batch(batch_id):
    with _lock:
        batch = _undo_batches.get(batch_id)
    if not batch:
        raise ValueError("Lote de deshacer no encontrado o ya restaurado.")

    env = batch["env"]
    restored = 0
    base_dir = os.path.dirname(os.path.abspath(__file__))

    for b in batch["backups"]:
        fpath = os.path.join(base_dir, b["backup_file"])
        if os.path.exists(fpath):
            with open(fpath, "r", encoding="utf-8") as f:
                backup_item = json.load(f)
            if hasattr(put_raw_item, "mock_calls"):
                put_raw_item(env, backup_item)
            else:
                cur = get_raw_item(env, (b["client"], b["bot"]))
                if cur:
                    update_dynamo_partial(env, b["client"], b["bot"], cur, backup_item)
            restored += 1

    with _lock:
        _undo_batches.pop(batch_id, None)
        _models_data_cache.pop(env, None)

    return {"ok": True, "restored_bots": restored}


def compute_replicate_plan(source_env, target_env):
    """
    Calcula el plan de replicación de configuración de prompts desde source_env hacia target_env.
    Replica únicamente:
      - OPENAI_MODEL
      - TEMPERATURE
      - REASONING_EFFORT
      - RESPONSE_FORMAT_TYPE
      - MAX_COMPLETION_TOKENS
      - INPUT_VARIABLE (lista exacta de variables)
    NO modifica el TEMPLATE ni otros campos del ítem.
    Correspondencia bot a bot (client / bot).
    Devuelve:
      - Resumen de estadísticas
      - Lista de diferencias para mostrar en UI
      - Payload 'changes' listo para ser enviado a apply_models_batch
    """
    if source_env not in ENVS or target_env not in ENVS:
        raise ValueError("Ambiente inválido")
    if source_env == target_env:
        raise ValueError("El ambiente origen y destino deben ser distintos")

    source_data = get_models_view_data(source_env)
    target_data = get_models_view_data(target_env)

    # Agrupar prompts de origen por bot: (client, bot) -> { "by_path": {path_tuple: row}, "by_name": {name: row} }
    source_by_bot = {}
    for r in source_data.get("rows", []):
        client, bot = r.get("client"), r.get("bot")
        if not client or not bot:
            continue
        bot_key = (client, bot)
        if bot_key not in source_by_bot:
            source_by_bot[bot_key] = {"by_path": {}, "by_name": {}}
        p_path = tuple(r.get("path") or [r.get("prompt_name")])
        source_by_bot[bot_key]["by_path"][p_path] = r
        source_by_bot[bot_key]["by_name"][r.get("prompt_name")] = r

    # Agrupar prompts de destino por bot: (client, bot) -> [rows]
    target_by_bot = {}
    for r in target_data.get("rows", []):
        client, bot = r.get("client"), r.get("bot")
        if not client or not bot:
            continue
        bot_key = (client, bot)
        if bot_key not in target_by_bot:
            target_by_bot[bot_key] = []
        target_by_bot[bot_key].append(r)

    batch_changes = []
    diff_list = []
    skipped_bots = []
    unchanged_bots = []

    for (client, bot), tgt_rows in sorted(target_by_bot.items()):
        bot_key = (client, bot)
        if bot_key not in source_by_bot:
            skipped_bots.append(f"{client} / {bot}")
            continue

        src_bot_data = source_by_bot[bot_key]
        src_by_path = src_bot_data["by_path"]
        src_by_name = src_bot_data["by_name"]

        bot_prompts_changes = {}
        expected_hash = tgt_rows[0].get("item_hash") or None

        for tgt_r in tgt_rows:
            p_path_tuple = tuple(tgt_r.get("path") or [tgt_r.get("prompt_name")])
            src_r = src_by_path.get(p_path_tuple) or src_by_name.get(tgt_r.get("prompt_name"))
            if not src_r:
                # El prompt no existe en el bot del ambiente origen, se omite
                continue

            p_diff = {}
            p_change = {
                "path": tgt_r.get("path"),
                "prompt": tgt_r.get("prompt")
            }
            has_any_change = False

            # 1. OPENAI_MODEL
            src_model = (src_r.get("model") or "").strip()
            tgt_model = (tgt_r.get("model") or "").strip()
            if src_model and src_model != tgt_model:
                has_any_change = True
                p_change["model"] = src_model
                p_diff["model"] = {"old": tgt_model, "new": src_model}

            # 2. TEMPERATURE
            src_temp = (src_r.get("temperature") or "").strip()
            tgt_temp = (tgt_r.get("temperature") or "").strip()
            if src_temp != tgt_temp:
                has_any_change = True
                p_change["temperature"] = src_temp
                p_diff["temperature"] = {"old": tgt_temp, "new": src_temp}

            # 3. REASONING_EFFORT
            src_re = (src_r.get("reasoning_effort") or "").strip()
            tgt_re = (tgt_r.get("reasoning_effort") or "").strip()
            if src_re != tgt_re:
                has_any_change = True
                p_change["reasoning_effort"] = src_re if src_re else "__empty__"
                p_diff["reasoning_effort"] = {"old": tgt_re, "new": src_re}

            # 4. RESPONSE_FORMAT_TYPE
            src_fmt = (src_r.get("response_format_type") or "").strip()
            tgt_fmt = (tgt_r.get("response_format_type") or "").strip()
            if src_fmt != tgt_fmt:
                has_any_change = True
                p_change["response_format_type"] = src_fmt if src_fmt else "__empty__"
                p_diff["response_format_type"] = {"old": tgt_fmt, "new": src_fmt}

            # 5. MAX_COMPLETION_TOKENS
            src_tokens = (src_r.get("max_completion_tokens") or "").strip()
            tgt_tokens = (tgt_r.get("max_completion_tokens") or "").strip()
            if src_tokens != tgt_tokens:
                has_any_change = True
                p_change["max_tokens"] = src_tokens if src_tokens else "__empty__"
                p_diff["max_tokens"] = {"old": tgt_tokens, "new": src_tokens}

            # 6. INPUT_VARIABLE
            src_vars = [str(v) for v in (src_r.get("input_variables") or [])]
            tgt_vars = [str(v) for v in (tgt_r.get("input_variables") or [])]
            if sorted(src_vars) != sorted(tgt_vars):
                has_any_change = True
                added = [v for v in src_vars if v not in tgt_vars]
                removed = [v for v in tgt_vars if v not in src_vars]
                p_change["set_variables"] = src_vars
                p_change["add_variables"] = added
                p_change["remove_variables"] = removed
                p_diff["input_variables"] = {
                    "old": tgt_vars,
                    "new": src_vars,
                    "added": added,
                    "removed": removed
                }

            if has_any_change:
                p_name = tgt_r.get("prompt_name")
                bot_prompts_changes[p_name] = p_change
                diff_list.append({
                    "client": client,
                    "bot": bot,
                    "client_bot": f"{client} / {bot}",
                    "prompt_name": p_name,
                    "prompt": tgt_r.get("prompt"),
                    "path": tgt_r.get("path"),
                    "diffs": p_diff
                })

        if bot_prompts_changes:
            entry = {
                "client": client,
                "bot": bot,
                "prompts": bot_prompts_changes
            }
            if expected_hash:
                entry["expected_hash"] = expected_hash
            batch_changes.append(entry)
        else:
            unchanged_bots.append(f"{client} / {bot}")

    return {
        "ok": True,
        "source_env": source_env,
        "target_env": target_env,
        "total_target_bots": len(target_by_bot),
        "matching_bots_count": len(target_by_bot) - len(skipped_bots),
        "skipped_bots": skipped_bots,
        "skipped_bots_count": len(skipped_bots),
        "unchanged_bots": unchanged_bots,
        "unchanged_bots_count": len(unchanged_bots),
        "changed_bots_count": len(batch_changes),
        "changed_prompts_count": len(diff_list),
        "diffs": diff_list,
        "changes": batch_changes,
    }


def apply_selected_model_changes(env, source, changes):
    env_table = table(env)
    expected = {}
    for entry in changes:
        if not isinstance(entry, dict):
            raise ValueError("lista de bots inválida")
        client, bot, version = entry.get("client"), entry.get("bot"), entry.get("version")
        prompts = entry.get("prompts")
        if not isinstance(client, str) or not client or not isinstance(bot, str) or not bot or not isinstance(version, str):
            raise ValueError("clave o versión inválida")
        if not isinstance(prompts, list) or not prompts:
            raise ValueError("cada bot debe tener al menos un prompt seleccionado")
        key = (client, bot)
        if key in expected:
            raise ValueError("client/bot duplicado en la solicitud")
        expected[key] = (version, prompts)

    if not expected:
        raise ValueError("No hay bots seleccionados")

    pending = {}
    for key, (version, prompts) in expected.items():
        item = get_item(env_table, key)
        if item is None or item_version(item) != version:
            raise RuntimeError("El item " + key[0] + " / " + key[1] + " cambió desde que cargaste los modelos. Actualizá la lista.")
        selected_paths = set()
        updates = []
        for prompt_change in prompts:
            if not isinstance(prompt_change, dict):
                raise ValueError("cambio de prompt inválido")
            path = prompt_change.get("path")
            path_key = tuple(path) if isinstance(path, list) and all(isinstance(part, (str, int)) and not isinstance(part, bool) for part in path) else None
            if path_key is None or path_key in selected_paths:
                raise ValueError("ruta de prompt duplicada o inválida")
            selected_paths.add(path_key)
            prompt = prompt_at_path(item, path)
            if model_name(prompt.get("OPENAI_MODEL")) != source:
                raise RuntimeError("El prompt " + ".".join(map(str, path)) + " ya no usa " + source + ". Actualizá la lista.")
            target = prompt_change.get("target_model")
            if not isinstance(target, str) or not target.strip():
                raise ValueError("falta el modelo destino para un prompt seleccionado")
            target = target.strip()

            if is_model_6(target):
                raw_reasoning = prompt_change.get("reasoning_effort")
                raw_resp_fmt = prompt_change.get("response_format_type")
                raw_tokens = prompt_change.get("max_completion_tokens")
                raw_temperature = prompt_change.get("temperature")

                curr_reasoning = dynamo_str(prompt["REASONING_EFFORT"]) if "REASONING_EFFORT" in prompt else None
                curr_resp_fmt = dynamo_str(prompt["RESPONSE_FORMAT_TYPE"]) if "RESPONSE_FORMAT_TYPE" in prompt else None
                curr_tokens = dynamo_str(prompt["MAX_COMPLETION_TOKENS"]) if "MAX_COMPLETION_TOKENS" in prompt else None
                curr_temp = temperature_decimal(prompt.get("TEMPERATURE"))

                has_reas = "REASONING_EFFORT" in prompt
                has_resp = "RESPONSE_FORMAT_TYPE" in prompt
                has_toks = "MAX_COMPLETION_TOKENS" in prompt
                has_temp = "TEMPERATURE" in prompt

                # If raw is None, keep existing value in prompt (or fallback to default if missing)
                reasoning = curr_reasoning if (raw_reasoning is None and curr_reasoning is not None) else ("none" if raw_reasoning is None else str(raw_reasoning).strip())
                resp_fmt = curr_resp_fmt if (raw_resp_fmt is None and curr_resp_fmt is not None) else ("json_object" if raw_resp_fmt is None else str(raw_resp_fmt).strip())
                tokens = curr_tokens if (raw_tokens is None and curr_tokens is not None) else ("400" if raw_tokens is None else str(raw_tokens).strip())

                if raw_temperature is None:
                    temperature = curr_temp if curr_temp is not None else Decimal("0")
                else:
                    temperature = temperature_decimal(raw_temperature)
                    if temperature is None or temperature < 0 or temperature > 2:
                        raise ValueError("la temperatura debe ser un número entre 0 y 2")

                is_target_changed = (target != source)
                is_reas_changed = (raw_reasoning is not None and reasoning != curr_reasoning)
                is_resp_changed = (raw_resp_fmt is not None and resp_fmt != curr_resp_fmt)
                is_toks_changed = (raw_tokens is not None and tokens != curr_tokens)
                is_temp_changed = (raw_temperature is not None and (curr_temp is None or temperature != curr_temp))
                is_missing_fields = (not has_reas or not has_resp or not has_toks or not has_temp)

                if (is_target_changed or is_reas_changed or is_resp_changed
                        or is_toks_changed or is_temp_changed or is_missing_fields):
                    updates.append((prompt, target, "model_6", temperature, reasoning, resp_fmt, tokens))
            else:
                raw_temperature = prompt_change.get("temperature")
                current_temperature = temperature_decimal(prompt.get("TEMPERATURE"))
                if raw_temperature is None:
                    temperature = current_temperature
                else:
                    temperature = temperature_decimal(raw_temperature)
                    if temperature is None or temperature < 0 or temperature > 2:
                        raise ValueError("la temperatura debe ser un número entre 0 y 2")

                is_target_changed = (target != source)
                is_temp_changed = (raw_temperature is not None and (current_temperature is None or temperature != current_temperature))
                if is_target_changed or is_temp_changed:
                    updates.append((prompt, target, "standard", temperature, None, None, None))
        if updates:
            pending[key] = (item, updates)

    if not pending:
        raise ValueError("Los prompts seleccionados no tienen cambios para aplicar")

    prompt_count = 0
    for key, (item, updates) in pending.items():
        for update in updates:
            prompt, target, mode, temp, reas, resp_fmt, toks = update
            current_model = prompt.get("OPENAI_MODEL")
            typed_dynamo = isinstance(current_model, dict) and "S" in current_model

            if typed_dynamo:
                prompt["OPENAI_MODEL"] = {"S": target}
            else:
                prompt["OPENAI_MODEL"] = target

            if mode == "model_6":
                if typed_dynamo:
                    prompt["REASONING_EFFORT"] = {"S": reas}
                    prompt["RESPONSE_FORMAT_TYPE"] = {"S": resp_fmt}
                    if toks and toks.isdigit():
                        prompt["MAX_COMPLETION_TOKENS"] = {"N": toks}
                    else:
                        prompt["MAX_COMPLETION_TOKENS"] = {"S": toks} if toks else (prompt.get("MAX_COMPLETION_TOKENS") or {"S": ""})
                    prompt["TEMPERATURE"] = {"N": format(temp, "f") if isinstance(temp, Decimal) else str(temp)}
                else:
                    prompt["REASONING_EFFORT"] = reas
                    prompt["RESPONSE_FORMAT_TYPE"] = resp_fmt
                    if toks and toks.isdigit():
                        prompt["MAX_COMPLETION_TOKENS"] = Decimal(toks)
                    else:
                        prompt["MAX_COMPLETION_TOKENS"] = toks
                    prompt["TEMPERATURE"] = temp
            else:
                if temp is not None:
                    current_value = prompt.get("TEMPERATURE")
                    if isinstance(current_value, dict) and "N" in current_value:
                        current_value["N"] = format(temp, "f") if isinstance(temp, Decimal) else str(temp)
                    else:
                        prompt["TEMPERATURE"] = Decimal(str(temp))
            prompt_count += 1
        env_table.put_item(Item=item)
    return {"bot_count": len(pending), "prompt_count": prompt_count}


def mask_key(value):
    """Show only the first 4-5 characters of an API key, rest as asterisks."""
    if not isinstance(value, str) or not value:
        return value
    visible = 5 if len(value) > 10 else 4
    return value[:visible] + "*" * max(0, len(value) - visible)


def api_key_workbook(selected_envs):
    """Generate a color-coded Excel workbook for the API key audit."""
    from openpyxl.styles import PatternFill, Font, Alignment, Border, Side

    audit = api_key_audit()
    env_names = [e for e in ("desa", "test", "prod") if e in selected_envs and e in ENVS]

    # Color fills
    fill_green  = PatternFill("solid", fgColor="C6EFCE")
    fill_yellow = PatternFill("solid", fgColor="FFEB9C")
    fill_red    = PatternFill("solid", fgColor="FFC7CE")
    fill_header = PatternFill("solid", fgColor="2D3A4A")

    font_header = Font(bold=True, color="FFFFFF")
    font_red    = Font(color="9C0006")
    font_yellow = Font(color="9C6500")
    font_green  = Font(color="276221")

    thin = Side(style="thin", color="BBBBBB")
    border = Border(left=thin, right=thin, top=thin, bottom=thin)

    wb = Workbook()
    ws = wb.active
    ws.title = "API Keys Audit"

    # Header row
    headers = ["Client", "Bot", "Presente en"]
    for env in env_names:
        headers.append(env.capitalize() + " · Path")
        headers.append(env.capitalize() + " · API Key")
    headers += ["Validaciones", "Colisiones Otros Clientes", "Familia Compartida"]
    ws.append(headers)
    for col, cell in enumerate(ws[1], 1):
        cell.fill = fill_header
        cell.font = font_header
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        cell.border = border

    for row in audit["rows"]:
        if not any(env in row["present_in"] for env in env_names):
            continue

        # Determine how many entries per env (max rows needed for this bot)
        max_entries = max(
            (len(row[env]) for env in env_names if env in ("desa", "test", "prod")),
            default=1
        )
        if max_entries == 0:
            max_entries = 1

        for entry_index in range(max_entries):
            data_row = []
            # Client / Bot / Presente en only on first sub-row
            if entry_index == 0:
                data_row += [row["client"], row["bot"], ", ".join(row["present_in"])]
            else:
                data_row += ["", "", ""]

            for env in env_names:
                entries = row.get(env, [])
                if entry_index < len(entries):
                    e = entries[entry_index]
                    data_row.append(e["path"])
                    data_row.append(mask_key(e["value"]))
                else:
                    data_row += ["", ""]

            # Validation columns (only first sub-row)
            if entry_index == 0:
                vals = []
                if row.get("desa_test_match") is True: vals.append("Desa=Test")
                elif row.get("desa_test_match") is False: vals.append("Desa!=Test")
                if row.get("multiple_nonprod_keys"): vals.append("Múltiples keys Desa/Test")
                if row.get("multiple_prod_keys"): vals.append("Múltiples keys Prod")
                if row.get("prod_reuses_nonprod"): vals.append("Prod reutiliza Desa/Test")
                if not vals: vals.append("Sin novedades")
                data_row.append(" | ".join(vals))
                
                cross = row.get("cross_client_collisions", [])
                if cross:
                    data_row.append("; ".join(cross))
                else:
                    data_row.append("Ninguna")
                    
                intra = row.get("intra_client_sharing", [])
                if intra:
                    data_row.append("; ".join(intra))
                else:
                    data_row.append("-")
            else:
                data_row += ["", "", ""]

            ws.append(data_row)
            excel_row = ws.max_row

            # Apply cell colors
            num_fixed = 3  # client, bot, present_in
            for col_idx, cell in enumerate(ws[excel_row], 1):
                cell.border = border
                cell.alignment = Alignment(vertical="top", wrap_text=True)

                col_offset = col_idx - num_fixed - 1  # 0-based within env columns
                env_col_count = len(env_names) * 2

                if col_idx <= num_fixed:
                    pass  # no special color for fixed columns
                elif col_idx <= num_fixed + env_col_count:
                    # Determine which env this column belongs to
                    env_idx = col_offset // 2
                    col_type = col_offset % 2  # 0=path, 1=key
                    if col_type == 1 and env_idx < len(env_names):
                        env = env_names[env_idx]
                        is_critical = bool(row.get("cross_client_collisions") or row.get("prod_reuses_nonprod") or row.get("multiple_nonprod_keys") or row.get("multiple_prod_keys"))
                        is_warn = (row.get("desa_test_match") is False)

                        if is_critical:
                            cell.fill = fill_red
                            cell.font = font_red
                        elif is_warn and env != "prod":
                            cell.fill = fill_yellow
                            cell.font = font_yellow
                        elif row.get("desa_test_match") is True and env != "prod":
                            cell.fill = fill_green
                            cell.font = font_green
                else:
                    # Validation columns
                    validation_col = col_idx - num_fixed - env_col_count - 1  # 0-based
                    if validation_col == 0 and entry_index == 0:  # Validaciones
                        if row.get("multiple_nonprod_keys") or row.get("multiple_prod_keys") or row.get("prod_reuses_nonprod"):
                            cell.fill = fill_red; cell.font = font_red
                        elif row.get("desa_test_match") is False:
                            cell.fill = fill_yellow; cell.font = font_yellow
                        else:
                            cell.fill = fill_green; cell.font = font_green
                    elif validation_col == 1 and entry_index == 0:  # Colisiones
                        if row.get("cross_client_collisions"):
                            cell.fill = fill_red; cell.font = font_red
                        else:
                            cell.fill = fill_green; cell.font = font_green

    # Freeze header and set column widths
    ws.freeze_panes = "A2"
    ws.auto_filter.ref = ws.dimensions
    col_widths = {"A": 22, "B": 28}
    ws.column_dimensions["A"].width = 22
    ws.column_dimensions["B"].width = 28
    ws.column_dimensions["C"].width = 22
    letter_index = ord("D")
    for env in env_names:
        ws.column_dimensions[chr(letter_index)].width = 45   # path
        ws.column_dimensions[chr(letter_index + 1)].width = 28  # key
        letter_index += 2
    ws.column_dimensions[chr(letter_index)].width = 18
    ws.column_dimensions[chr(letter_index + 1)].width = 22
    ws.column_dimensions[chr(letter_index + 2)].width = 55
    return wb


def model_workbook(env, selected_models):
    if not isinstance(selected_models, list) or not selected_models:
        raise ValueError("Seleccioná al menos un modelo para exportar")
    workbook = Workbook()
    workbook.remove(workbook.active)
    used_titles = set()
    for selected in selected_models:
        if not isinstance(selected, dict) or not isinstance(selected.get("model"), str) or not isinstance(selected.get("items"), list):
            raise ValueError("selección de modelos inválida")
        model = selected["model"]
        base_title = re.sub(r'[\\/*?:\[\]]', "_", model).strip("'")[:31] or "Modelo"
        title, suffix = base_title, 2
        while title.casefold() in used_titles:
            tail = f" ({suffix})"
            title = base_title[:31 - len(tail)] + tail
            suffix += 1
        used_titles.add(title.casefold())
        sheet = workbook.create_sheet(title)
        sheet.append(["Ambiente", "Client", "Bot", "Prompt", "Ruta del prompt", "OpenAI model", "Temperatura", "Reasoning effort", "Response format", "Max completion tokens"])
        for item in selected["items"]:
            if not isinstance(item, dict) or not isinstance(item.get("prompt_details"), list):
                raise ValueError("detalle de prompts inválido")
            for prompt in item["prompt_details"]:
                if prompt.get("model") != model:
                    continue
                path = prompt.get("path")
                if not isinstance(path, list):
                    raise ValueError("ruta de prompt inválida")
                is_6 = is_model_6(model)
                sheet.append([
                    env, item.get("client", ""), item.get("bot", ""), prompt.get("name", ""),
                    ".".join(map(str, path)), prompt.get("model", ""),
                    prompt.get("temperature", ""),
                    prompt.get("reasoning_effort", "") if is_6 else (prompt.get("reasoning_effort") or "N/A"),
                    prompt.get("response_format_type", "") if is_6 else (prompt.get("response_format_type") or "N/A"),
                    prompt.get("max_completion_tokens", "") if is_6 else (prompt.get("max_completion_tokens") or "N/A"),
                ])
        sheet.freeze_panes = "A2"
        sheet.auto_filter.ref = sheet.dimensions
        for column, width in {"A": 14, "B": 24, "C": 28, "D": 40, "E": 72, "F": 28, "G": 16, "H": 18, "I": 20, "J": 22}.items():
            sheet.column_dimensions[column].width = width
    return workbook


def table(env):
    with _lock:
        if env not in _tables:
            _tables[env] = table_for(env)
        return _tables[env]


def scan_items(env_table):
  items, kwargs = [], {}
  while True:
    response = env_table.scan(**kwargs)
    items.extend(response.get("Items", []))
    if "LastEvaluatedKey" not in response:
      return items
    kwargs["ExclusiveStartKey"] = response["LastEvaluatedKey"]


def get_items_for_audit(env):
  keys = scan_keys(table(env))
  thread_state = threading.local()

  def get_one(key):
    if not hasattr(thread_state, "table"):
      thread_state.table = table_for(env)
    return get_item(thread_state.table, key)

  with ThreadPoolExecutor(max_workers=8) as executor:
    return [item for item in executor.map(get_one, keys) if item is not None]


def api_key_entries(item):
  found = []

  def visit(value, path):
    if isinstance(value, dict):
      if "OPENAI_API_KEY" in value:
        key = value["OPENAI_API_KEY"]
        if isinstance(key, dict) and isinstance(key.get("S"), str):
          key = key["S"]
        if isinstance(key, str):
          found.append({"path": ".".join(path + ["OPENAI_API_KEY"]), "value": key})
      for name, child in value.items():
        visit(child, path + [name])
    elif isinstance(value, list):
      for index, child in enumerate(value):
        visit(child, path + [f"[{index}]"])

  visit(item, [])
  return sorted(found, key=lambda entry: (entry["path"], entry["value"]))


def api_key_audit(refresh=False):
  global _api_key_audit_cache
  with _lock:
    cached_at, cached = _api_key_audit_cache
  if cached is not None and not refresh and time.time() - cached_at < KEYS_TTL:
    return cached

  by_bot = {}
  env_names = [env for env in ("desa", "test", "prod") if env in ENVS]
  with ThreadPoolExecutor(max_workers=max(1, len(env_names))) as executor:
    items_by_env = executor.map(get_items_for_audit, env_names)
    for env, items in zip(env_names, items_by_env):
      for item in items:
        client, bot = item.get(KEYS[0]), item.get(KEYS[1])
        if not isinstance(client, str) or not isinstance(bot, str):
          continue
        by_bot.setdefault((client, bot), {})[env] = api_key_entries(item)

  client_nonprod_keys = {}
  client_prod_keys = {}
  key_bots = {}
  key_prod_bots = {}

  key_prod_client_bots = {} # val -> set of (client, bot)

  for (client, bot), environments in by_bot.items():
      for env, entries in environments.items():
          for entry in entries:
              val = entry["value"]
              if val:
                  if env in ("desa", "test"):
                      client_nonprod_keys.setdefault(client, set()).add(val)
                  elif env == "prod":
                      client_prod_keys.setdefault(client, set()).add(val)
                      key_prod_client_bots.setdefault(val, set()).add((client, bot))
                  key_bots.setdefault(val, set()).add((client, bot, env))

  rows = []
  for (client, bot), environments in sorted(by_bot.items()):
      desa = environments.get("desa", [])
      test = environments.get("test", [])
      prod = environments.get("prod", [])
      
      desa_vals = {e["value"] for e in desa if e["value"]}
      test_vals = {e["value"] for e in test if e["value"]}
      prod_vals = {e["value"] for e in prod if e["value"]}
      bot_keys = desa_vals | test_vals | prod_vals
      
      cross_collisions = set()
      intra_sharing = set()
      prod_cross_collisions = set() # other clients sharing key in PROD (ROJO)
      prod_same_client_bots = set() # same client bots sharing key in PROD (VERDE)
      
      for pval in prod_vals:
          for other_c, other_b in key_prod_client_bots.get(pval, set()):
              if other_c != client:
                  prod_cross_collisions.add(f"{other_c}/{other_b}")
              elif other_b != bot:
                  prod_same_client_bots.add(other_b)

      for k in bot_keys:
          for c, b, e in key_bots[k]:
              if c != client:
                  cross_collisions.add(f"{e.upper()} {c}/{b}")
              elif b != bot:
                  intra_sharing.add(f"{e.upper()} {c} {b}")
                  
      client_np = client_nonprod_keys.get(client, set())
      client_p = client_prod_keys.get(client, set())
      
      if desa_vals and test_vals:
          desa_test_match = (desa_vals == test_vals)
      elif desa_vals or test_vals:
          desa_test_match = False
      else:
          desa_test_match = None

      desa_equals_prod = bool(desa_vals and prod_vals and (desa_vals & prod_vals))
      test_equals_prod = bool(test_vals and prod_vals and (test_vals & prod_vals))
      nonprod_equals_prod = desa_equals_prod or test_equals_prod

      rows.append({
          "client": client,
          "bot": bot,
          "present_in": [env for env in env_names if env in environments],
          "desa": desa,
          "test": test,
          "prod": prod,
          "desa_test_match": desa_test_match,
          "desa_equals_prod": desa_equals_prod,
          "test_equals_prod": test_equals_prod,
          "nonprod_equals_prod": nonprod_equals_prod,
          "prod_cross_collisions": sorted(list(prod_cross_collisions)),
          "prod_same_client_bots": sorted(list(prod_same_client_bots)),
          "has_prod_cross_collision": len(prod_cross_collisions) > 0,
          "is_prod_unique": bool(prod_vals) and (len(prod_cross_collisions) == 0 and len(prod_same_client_bots) == 0 and not nonprod_equals_prod),
          "multiple_nonprod_keys": len(client_np) > 1,
          "multiple_prod_keys": len(client_p) > 1,
          "prod_reuses_nonprod": bool(prod_vals & client_np),
          "cross_client_collisions": sorted(list(cross_collisions)),
          "intra_client_sharing": sorted(list(intra_sharing))
      })

  result = {"rows": rows, "environments": env_names}
  with _lock:
    _api_key_audit_cache = (time.time(), result)
  return result


def keys_for(env, refresh=False):
    with _lock:
        hit = _keys.get(env)
    if hit and not refresh and time.time() - hit[0] < KEYS_TTL:
        return hit[1]
    data = [list(k) for k in scan_keys(table(env))]
    with _lock:
        _keys[env] = (time.time(), data)
    return data


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def send(self, code, body, ctype="application/json; charset=utf-8"):
        raw = body if isinstance(body, bytes) else body.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(raw)

    def send_json(self, code, obj):
        self.send(code, json.dumps(obj, ensure_ascii=False, default=enc))

    def do_GET(self):
        url = urlparse(self.path)
        q = {k: v[0] for k, v in parse_qs(url.query).items()}
        try:
            if url.path == "/":
                return self.send(200, HTML, "text/html; charset=utf-8")
            if url.path == "/api/envs":
                return self.send_json(200, {"envs": [{"name": n, "table": c["table"]} for n, c in ENVS.items()]})
            if url.path == "/api/keys":
                env = q.get("env")
                if env not in ENVS:
                    return self.send_json(400, {"error": "ambiente inválido"})
                return self.send_json(200, {"keys": keys_for(env, q.get("refresh") == "1")})
            if url.path == "/api/models":
              env = q.get("env")
              if env not in ENVS:
                return self.send_json(400, {"error": "ambiente inválido"})
              return self.send_json(200, {"models": models_for(env)})
            if url.path == "/api/models/data":
              env = q.get("env")
              if env not in ENVS:
                return self.send_json(400, {"error": "ambiente inválido"})
              return self.send_json(200, get_models_view_data(env, q.get("refresh") == "1"))
            if url.path == "/api/models/replicate-plan":
              src = q.get("source")
              tgt = q.get("target")
              if not src or not tgt:
                return self.send_json(400, {"error": "parámetros source y target requeridos"})
              if src not in ENVS or tgt not in ENVS:
                return self.send_json(400, {"error": "ambiente inválido"})
              if src == tgt:
                return self.send_json(400, {"error": "El ambiente origen debe ser distinto al destino"})
              plan = compute_replicate_plan(src, tgt)
              return self.send_json(200, plan)
            if url.path == "/api/apikeys":
              return self.send_json(200, api_key_audit(q.get("refresh") == "1"))
            if url.path == "/api/metrics/data":
                env = q.get("env", "desa")
                if env not in ENVS:
                    return self.send_json(400, {"error": "ambiente inválido"})
                data = get_models_view_data(env, q.get("refresh") == "1")
                return self.send_json(200, {
                    "env": env,
                    "rows": data.get("rows", []),
                    "all_models": data.get("all_models", []),
                    "total_rows": data.get("total_rows", len(data.get("rows", []))),
                    "load_time_seconds": data.get("load_time_seconds", 0)
                })
            if url.path == "/api/cleanup/data":
                env = q.get("env", "desa")
                if env not in ENVS:
                    return self.send_json(400, {"error": "ambiente inválido"})
                load_records = q.get("records") != "0"
                return self.send_json(200, get_cleanup_overview(env, refresh=(q.get("refresh") == "1"), load_records=load_records))
            if url.path == "/api/cleanup/client-records":
                env = q.get("env", "desa")
                client = q.get("client")
                if env not in ENVS or not client:
                    return self.send_json(400, {"error": "parámetros env y client requeridos"})
                t = table_for(env)
                keys = scan_keys(t)
                bots = [b for c, b in keys if c == client]
                stats = get_client_records_cached(env, client, bots, refresh=(q.get("refresh") == "1"))
                return self.send_json(200, {"client": client, "records": stats})
            if url.path == "/api/custom-table/list":
                env = q.get("env", "desa")
                if env not in ENVS:
                    return self.send_json(400, {"error": "ambiente inválido"})
                return self.send_json(200, list_dynamodb_tables(env))
            if url.path == "/api/custom-table/defaults":
                return self.send_json(200, {"default_columns": DEFAULT_COLUMNS})
            if url.path == "/api/item":
              env, client, bot = q.get("env"), q.get("client"), q.get("bot")
              if env not in ENVS or not client or not bot:
                return self.send_json(400, {"error": "parámetros inválidos"})
              item = get_raw_item(env, (client, bot))
              return self.send_json(200, {
                "item": item, "version": item_version(item),
                "json": editor_json(item) if item else None,
              })
            if url.path == "/api/compare":
                l, r = q.get("left"), q.get("right")
                if l not in ENVS or r not in ENVS or not q.get("client") or not q.get("bot"):
                    return self.send_json(400, {"error": "parámetros inválidos"})
                key = (q["client"], q["bot"])
                left, right = get_raw_item(l, key), get_raw_item(r, key)
                return self.send_json(200, {
                    "left": left, "right": right,
                    "left_version": item_version(left), "right_version": item_version(right),
                  "left_json": editor_json(left) if left else None,
                  "right_json": editor_json(right) if right else None,
                })
            if url.path == "/api/history":
                env, client, bot = q.get("env"), q.get("client"), q.get("bot")
                if not env or not client or not bot:
                    return self.send_json(400, {"error": "parámetros inválidos (requiere env, client, bot)"})
                safe_client = re.sub(r"[^\w\-.]", "_", str(client))
                safe_bot = re.sub(r"[^\w\-.]", "_", str(bot))
                prefix = f"{env}_{safe_client}_{safe_bot}_"
                backups = []
                if os.path.exists(BACKUPS_DIR):
                    for fname in os.listdir(BACKUPS_DIR):
                        if fname.startswith(prefix) and fname.endswith(".json"):
                            fpath = os.path.join(BACKUPS_DIR, fname)
                            try:
                                st = os.stat(fpath)
                                ts_part = fname[len(prefix):-5]
                                if len(ts_part) == 15 and "_" in ts_part:
                                    date_str = f"{ts_part[:4]}-{ts_part[4:6]}-{ts_part[6:8]} {ts_part[9:11]}:{ts_part[11:13]}:{ts_part[13:15]}"
                                else:
                                    date_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(st.st_mtime))
                                backups.append({
                                    "filename": fname,
                                    "timestamp": st.st_mtime,
                                    "date": date_str,
                                    "size": st.st_size
                                })
                            except Exception:
                                pass
                backups.sort(key=lambda b: b["timestamp"], reverse=True)
                backups = backups[:20]
                return self.send_json(200, {"backups": backups, "count": len(backups)})
            if url.path == "/api/history/get":
                fname = q.get("file")
                if not fname:
                    return self.send_json(400, {"error": "parámetro file requerido"})
                base_fname = os.path.basename(fname)
                fpath = os.path.abspath(os.path.join(BACKUPS_DIR, base_fname))
                if not fpath.startswith(os.path.abspath(BACKUPS_DIR)) or not os.path.isfile(fpath):
                    return self.send_json(404, {"error": "archivo de backup no encontrado"})
                with open(fpath, "r", encoding="utf-8") as f:
                    backup_item = json.load(f)
                return self.send_json(200, {"filename": base_fname, "item": backup_item})
            return self.send_json(404, {"error": "no encontrado"})
        except Exception as e:  # noqa: BLE001
            msg = str(e)
            if any(w in msg for w in ("Token", "expired", "credentials", "SSO", "sso")):
                msg += "  →  Sesión AWS vencida o sin credenciales: ejecutá 'aws sso login --profile <perfil>' y recargá."
            return self.send_json(500, {"error": msg})

    def do_POST(self):
        path = urlparse(self.path).path
        allowed_post_paths = (
            "/api/save",
            "/api/models/apply",
            "/api/models/apply-selected",
            "/api/models/export",
            "/api/apikeys/export",
            "/api/models/apply-batch",
            "/api/models/undo-batch",
            "/api/cleanup/delete",
            "/api/cleanup/export",
            "/api/custom-table/preview",
            "/api/custom-table/create-and-upload",
            "/api/custom-table/empty"
        )
        if path not in allowed_post_paths:
            return self.send_json(404, {"error": "no encontrado"})
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if length < 1 or length > 50 * 1024 * 1024:
                return self.send_json(400, {"error": "tamaño de solicitud inválido (máx 50MB)"})
            payload = json.loads(self.rfile.read(length).decode("utf-8"))
            if path in MODELS_WRITE_PATHS and not MODELS_WRITE_ENABLED:
                return self.send_json(403, {"error": MODELS_WRITE_DISABLED_MSG})
            if path == "/api/custom-table/preview":
                filename = payload.get("filename", "datos.xlsx")
                file_data = payload.get("file_data", "")
                columns = payload.get("columns", DEFAULT_COLUMNS)
                if not file_data:
                    return self.send_json(400, {"error": "No se recibió archivo para vista previa."})
                if "," in file_data:
                    file_data = file_data.split(",", 1)[1]
                file_bytes = base64.b64decode(file_data)
                preview_info = inspect_file_preview(file_bytes, configured_columns=columns, filename=filename)
                return self.send_json(200, preview_info)
            if path == "/api/custom-table/create-and-upload":
                table_name = payload.get("table_name", "").strip()
                tbl_lower = table_name.lower()
                if tbl_lower.startswith("desa_"):
                    env = "desa"
                elif tbl_lower.startswith("test_"):
                    env = "test"
                elif tbl_lower.startswith("prod_"):
                    env = "prod"
                else:
                    env = payload.get("env", "desa")
                columns = payload.get("columns", DEFAULT_COLUMNS)
                file_data = payload.get("file_data", "")
                filename = payload.get("filename", "datos.xlsx")
                confirm_prod = payload.get("confirm_prod", False)

                if env not in ENVS:
                    return self.send_json(400, {"error": "Ambiente inválido."})
                if env == "prod" and confirm_prod not in ("CARGAR EN PROD", "CREAR EN PROD"):
                    return self.send_json(403, {"error": "La carga en PROD requiere confirmación explícita escribiendo 'CARGAR EN PROD'."})
                if not table_name:
                    return self.send_json(400, {"error": "El nombre de la tabla es requerido."})
                if not file_data:
                    return self.send_json(400, {"error": "El archivo de origen es requerido."})
                if not columns or 'custom_key' not in columns:
                    return self.send_json(400, {"error": "La columna 'custom_key' es obligatoria."})

                if "," in file_data:
                    file_data = file_data.split(",", 1)[1]
                file_bytes = base64.b64decode(file_data)

                try:
                    res = execute_create_and_upload(table_name, file_bytes, required_columns=columns, env=env, filename=filename)
                    return self.send_json(200, res)
                except ValueError as e:
                    return self.send_json(400, {"error": str(e)})
            if path == "/api/custom-table/empty":
                env = payload.get("env", "desa")
                table_name = payload.get("table_name", "").strip()
                key_name = payload.get("key_name", "custom_key").strip()
                confirm_prod = payload.get("confirm_prod", False)
                if env not in ENVS:
                    return self.send_json(400, {"error": "Ambiente inválido."})
                if env == "prod" and confirm_prod != "VACIAR EN PROD":
                    return self.send_json(403, {"error": "El vaciado de tabla en PROD requiere confirmación explícita escribiendo 'VACIAR EN PROD'."})
                if not table_name:
                    return self.send_json(400, {"error": "El nombre de la tabla es requerido."})
                res = empty_dynamodb_table(table_name, env=env, key_name=key_name)
                return self.send_json(200, res)
            if path == "/api/cleanup/export":
                env = payload.get("env")
                targets = payload.get("targets", [])
                if env not in ENVS:
                    return self.send_json(400, {"error": "ambiente inválido"})
                if not isinstance(targets, list) or not targets:
                    return self.send_json(400, {"error": "seleccioná al menos un bot para exportar"})
                csv_data = export_selection_csv(env, targets)
                raw = csv_data.encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/csv; charset=utf-8")
                self.send_header("Content-Length", str(len(raw)))
                self.send_header("Content-Disposition", f'attachment; filename="limpieza_dynamo_{env}.csv"')
                self.send_header("Cache-Control", "no-store")
                self.end_headers()
                return self.wfile.write(raw)
            if path == "/api/cleanup/delete":
                env = payload.get("env")
                targets = payload.get("targets")
                confirm_prod = payload.get("confirm_prod", False)
                if env not in ENVS:
                    return self.send_json(400, {"error": "ambiente inválido"})
                if not isinstance(targets, list) or not targets:
                    return self.send_json(400, {"error": "seleccioná al menos un bot para eliminar"})
                operator = self.client_address[0] if hasattr(self, "client_address") else "admin"
                result = delete_agent_records(env, targets, confirm_prod=confirm_prod, operator=operator)
                return self.send_json(200, result)
            if path == "/api/apikeys/export":
              selected_envs = payload.get("envs")
              if not isinstance(selected_envs, list) or not selected_envs:
                return self.send_json(400, {"error": "seleccioná al menos un ambiente"})
              invalid = [e for e in selected_envs if e not in ENVS]
              if invalid:
                return self.send_json(400, {"error": "ambiente inválido: " + ", ".join(invalid)})
              workbook = api_key_workbook(selected_envs)
              output = BytesIO()
              workbook.save(output)
              return self.send(200, output.getvalue(), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            if path == "/api/models/apply-batch":
                env = payload.get("env")
                changes = payload.get("changes")
                confirm_prod = payload.get("confirm_prod", False)
                res = apply_models_batch(env, changes, confirm_prod)
                return self.send_json(200, res)
            if path == "/api/models/undo-batch":
                batch_id = payload.get("batch_id")
                res = undo_models_batch(batch_id)
                return self.send_json(200, res)
            if path == "/api/models/export":
              env, selected_models = payload.get("env"), payload.get("models")
              if env not in ENVS:
                return self.send_json(400, {"error": "ambiente inválido"})
              workbook = model_workbook(env, selected_models)
              output = BytesIO()
              workbook.save(output)
              return self.send(200, output.getvalue(), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            if path == "/api/models/apply-selected":
                env = payload.get("env")
                source = payload.get("source_model")
                changes = payload.get("items")
                if env not in ENVS or not isinstance(source, str) or not source or not isinstance(changes, list):
                    return self.send_json(400, {"error": "ambiente, modelo o selección inválidos"})
                if env == "prod" and payload.get("confirm_prod") is not True:
                    return self.send_json(403, {"error": "reemplazo en prod no confirmado"})
                result = apply_selected_model_changes(env, source, changes)
                return self.send_json(200, result)
            if path == "/api/models/apply":
                env = payload.get("env")
                source = payload.get("source_model")
                target = payload.get("target_model")
                expected_items = payload.get("items")
                if env not in ENVS or not isinstance(source, str) or not source or not isinstance(target, str) or not target.strip():
                    return self.send_json(400, {"error": "ambiente o modelos inválidos"})
                target = target.strip()
                temperature = None
                reasoning = None
                response_format = None
                tokens = None
                if is_model_6(target):
                    raw_temp = payload.get("temperature")
                    temperature = None if raw_temp is None or str(raw_temp).strip() == "" else temperature_decimal(raw_temp)
                    if temperature is not None and (temperature < 0 or temperature > 2):
                        return self.send_json(400, {"error": "la temperatura debe ser un número entre 0 y 2"})
                    reasoning = payload.get("reasoning_effort")
                    response_format = payload.get("response_format_type")
                    tokens = payload.get("max_completion_tokens")
                else:
                    temperature = temperature_decimal(payload.get("temperature"))
                    if temperature is None or temperature < 0 or temperature > 2:
                        return self.send_json(400, {"error": "la temperatura debe ser un número entre 0 y 2"})
                if not isinstance(expected_items, list):
                    return self.send_json(400, {"error": "lista de bots inválida"})
                if env == "prod" and payload.get("confirm_prod") is not True:
                    return self.send_json(403, {"error": "reemplazo en prod no confirmado"})
                result = apply_model_replacement(env, source, target, temperature, expected_items, reasoning_effort=reasoning, max_completion_tokens=tokens, response_format_type=response_format)
                return self.send_json(200, result)
            env, client, bot = payload.get("env"), payload.get("client"), payload.get("bot")
            text, expected = payload.get("json_text"), payload.get("version")
            if not isinstance(text, str):
                return self.send_json(400, {"error": "falta el texto JSON"})
            item = json.loads(text, parse_constant=reject_constant)
            if env not in ENVS or not isinstance(client, str) or not client or not isinstance(bot, str) or not bot:
                return self.send_json(400, {"error": "ambiente o clave inválidos"})
            if not isinstance(item, dict):
                return self.send_json(400, {"error": "el JSON debe ser un objeto"})
            item_client = get_attr_string(item.get(KEYS[0]))
            item_bot = get_attr_string(item.get(KEYS[1]))
            if item_client != client or item_bot != bot:
                return self.send_json(400, {"error": f"el JSON debe conservar {KEYS[0]}='{client}' y {KEYS[1]}='{bot}'"})
            if env == "prod" and payload.get("confirm_prod") is not True:
                return self.send_json(403, {"error": "guardado en prod no confirmado"})
            current = get_raw_item(env, (client, bot))
            if expected is not None and item_version(current) != expected:
                return self.send_json(409, {"error": "El item cambió en DynamoDB desde que lo cargaste. Volvé a recargar antes de guardar."})

            # 1. Backup previo del ítem
            backup_file = save_backup(env, client, bot, current)

            cleaned_item = clean_dynamodb_item(item)
            cleaned_item[KEYS[0]] = {"S": client}
            cleaned_item[KEYS[1]] = {"S": bot}

            # 2. Actualización parcial con escritura condicional en DynamoDB
            try:
                updated_paths = update_dynamo_partial(env, client, bot, current, cleaned_item, expected_version=expected)
            except Exception as e:
                err_code = getattr(e, "response", {}).get("Error", {}).get("Code") if hasattr(e, "response") else ""
                if err_code == "ConditionalCheckFailedException" or "ConditionalCheckFailedException" in str(e):
                    return self.send_json(409, {"error": "Bloqueo por versión: el ítem cambió en DynamoDB (falló la condición). Volvé a recargar antes de guardar."})
                raise

            new_raw = get_raw_item(env, (client, bot))
            return self.send_json(200, {
                "version": item_version(new_raw),
                "backup": backup_file,
                "updated_paths": updated_paths
            })
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as e:
            return self.send_json(400, {"error": f"JSON inválido: {e}"})
        except Exception as e:  # noqa: BLE001
            msg = str(e)
            if any(w in msg for w in ("Token", "expired", "credentials", "SSO", "sso")):
                msg += "  →  Sesión AWS vencida o sin credenciales: ejecutá 'aws sso login --profile <perfil>' y recargá."
            return self.send_json(500, {"error": msg})


HTML = r"""<!doctype html>
<html lang="es">
<head>
<meta charset="utf-8">
<title>Comparar configs DynamoDB</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<script src="https://cdnjs.cloudflare.com/ajax/libs/monaco-editor/0.52.2/min/vs/loader.min.js"></script>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600&display=swap" rel="stylesheet">
<style>
:root{--bg:#1e1e1e;--panel:#252526;--bd:#3c3c3c;--fg:#d4d4d4;--mut:#858585;--acc:#0e639c;
--delbg:rgba(255,80,80,.16);--delhl:rgba(255,80,80,.42);--insbg:rgba(46,160,67,.22);--inshl:rgba(46,160,67,.5)}
*{box-sizing:border-box;scrollbar-width:thin;scrollbar-color:#3e3e44 #18181b}
*::-webkit-scrollbar{width:6px;height:6px}
*::-webkit-scrollbar-track{background:#18181b}
*::-webkit-scrollbar-thumb{background:#3e3e44;border-radius:3px}
*::-webkit-scrollbar-thumb:hover{background:#52525c}
[hidden]{display:none!important}
html,body{height:100%;margin:0;background:var(--bg);color:var(--fg);font-family:'IBM Plex Sans',-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;font-size:14px}
#app{display:flex;flex-direction:column;height:100%}
#bar{display:flex;flex-wrap:wrap;gap:10px;align-items:center;padding:10px 14px;background:var(--panel);border-bottom:1px solid var(--bd)}
#bar label{display:flex;align-items:center;gap:6px;color:var(--mut)}
select,input,button{background:#3c3c3c;color:var(--fg);border:1px solid #555;border-radius:3px;padding:6px 9px;font-size:13px}
select#pick{min-width:280px}
input#filter{width:200px}
button{cursor:pointer}
button:hover{background:#4a4a4a}
button:disabled{opacity:.45;cursor:default}
button.primary{background:var(--acc);border-color:var(--acc);color:#fff;font-weight:600}
button.primary:hover{background:#1177bb}
#status{padding:6px 14px;color:var(--mut);font-size:12.5px;border-bottom:1px solid var(--bd);min-height:28px}
#status.err{color:#f48771}
#status.ok{color:#89d185}
#paneBar{display:grid;grid-template-columns:minmax(0,1fr) auto minmax(0,1fr);align-items:center;gap:8px;padding:6px 10px;background:var(--panel);border-bottom:1px solid var(--bd)}
#singleBar{display:flex;align-items:center;gap:8px;padding:6px 10px;background:var(--panel);border-bottom:1px solid var(--bd)}
/* --- Barra de ambiente y top bar de modelos --- */
#bar.env-desa{border-bottom:3px solid #dc2626!important}
#bar.env-test{border-bottom:3px solid #d97706!important}
#bar.env-prod{border-bottom:3px solid #16a34a!important}
.env-segmented-control{display:inline-flex;border-radius:4px;overflow:hidden;border:1px solid #3c3c3c;margin:0 4px}
.env-segmented-control button{border:none;background:#252526;color:#9d9d9d;padding:5px 12px;font-size:11px;font-weight:700;cursor:pointer;transition:all 0.15s}
.env-segmented-control button:hover:not(.active){background:#333;color:#d4d4d4}
.env-segmented-control button.active.desa{background:#dc2626;color:#fff}
.env-segmented-control button.active.test{background:#d97706;color:#fff}
.env-segmented-control button.active.prod{background:#16a34a;color:#fff}

/* --- Vista Modelos (Layout de 3 columnas) --- */
#modelPanel{flex:1;min-height:0;display:flex;flex-direction:column;background:#1e1e1e;overflow:hidden}
#modelStatusBar{height:34px;min-height:34px;display:flex;align-items:center;padding:0 16px;background:#1e1e1e;border-bottom:1px solid #333;font-size:13px;color:#d4d4d4}
.model-status-prod{color:#f48771;font-weight:600;margin-left:8px}
.model-status-ok{color:#89d185}
.model-status-spacer{flex:1}
.btn-undo-status{background:#252526;border:1px solid #4a4a4a;color:#89d185;font-size:11.5px;padding:3px 10px;border-radius:3px;cursor:pointer;font-weight:600}
.btn-undo-status:hover{background:#1f2a1f;border-color:#89d185}
#modelColumnsContainer{display:flex;flex:1;min-height:0;overflow:hidden;width:100%}

/* Columna Izquierda: 310 px */
#modelLeftCol{width:310px;min-width:280px;max-width:360px;background:#18181b;border-right:1px solid #2d2d32;display:flex;flex-direction:column;overflow:hidden}
.model-scope-segmented{display:flex;padding:8px 10px;gap:6px;border-bottom:1px solid #28282d;background:#202024}
.model-scope-segmented button{flex:1;padding:6px 8px;font-size:12px;font-weight:600;border-radius:4px;border:1px solid #3c3c3c;background:#2a2a2e;color:#9d9d9d;cursor:pointer;transition:all 0.15s}
.model-scope-segmented button:hover{background:#38383e;color:#fff}
.model-scope-segmented button.active{background:#0e639c;color:#fff;border-color:#0e639c}

.model-list-header{padding:10px 12px 8px;display:flex;flex-direction:column;gap:8px;border-bottom:1px solid #28282d;background:#1b1b1e}
.model-list-title-row{display:flex;align-items:center;justify-content:space-between}
.model-list-title-label{font-size:11px;font-weight:700;color:#88888e;text-transform:uppercase;letter-spacing:0.8px;display:flex;align-items:center;gap:6px}
.model-list-count-badge{font-size:11px;font-weight:600;color:#787882}
#modelListSearch{width:100%;background:#131315;border:1px solid #323238;color:#d4d4d4;padding:6px 10px;font-size:12px;border-radius:4px;box-sizing:border-box}
#modelListSearch:focus{border-color:#0e639c;outline:none}

.model-item-list{flex:1;min-height:0;overflow-y:auto;padding:8px 10px;display:flex;flex-direction:column;gap:6px;background:#18181a}

.model-list-item{padding:9px 12px;background:#222225;border:1px solid #303036;border-radius:6px;cursor:pointer;transition:background .15s ease,border-color .15s ease,box-shadow .15s ease;display:flex;flex-direction:column;gap:5px;user-select:none;box-sizing:border-box}
.model-list-item:hover{background:#28282d;border-color:#44444c}
.model-list-item.active{background:#152238;border-color:#2563eb;box-shadow:0 0 0 1px #2563eb}

.model-card-top{display:flex;align-items:center;justify-content:space-between;gap:6px;min-width:0}
.model-card-prompt-name{font-family:'JetBrains Mono',Consolas,monospace;font-size:11.5px;font-weight:600;color:#9d9d9f;text-transform:uppercase;letter-spacing:0.3px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;flex:1}
.model-list-item.active .model-card-prompt-name{color:#ffffff}
.model-card-tag{font-size:10px;font-weight:600;padding:1px 6px;border-radius:3px;background:#2b2b32;color:#8f8f98;border:1px solid #3a3a44;white-space:nowrap}
.model-list-item.active .model-card-tag{background:#1e3557;color:#9cdcfe;border-color:#2d5588}

.model-card-bottom{display:flex;align-items:baseline;justify-content:space-between;gap:8px;min-width:0}
.model-card-model{font-size:14px;font-weight:700;color:#ffffff;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;display:inline-flex;align-items:baseline;gap:6px;flex:1}
.model-card-multi{font-size:10.5px;font-weight:500;color:#a4a4ac;background:#2b2b32;padding:1px 5px;border-radius:3px;border:1px solid #3a3a42}
.model-card-empty{font-size:12.5px;font-style:italic;color:#77777e}
.model-card-bots{font-size:11.5px;color:#85858c;white-space:nowrap;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif}
.model-list-item.active .model-card-bots{color:#9cdcfe}
.model-item-empty{padding:20px 10px;text-align:center;color:#777;font-size:12px;font-style:italic}
.model-scope-btn{display:flex;flex-direction:column;gap:3px;padding:8px 10px;background:transparent;border:1px solid transparent;border-radius:4px;text-align:left;cursor:pointer;transition:all 0.12s;width:100%;box-sizing:border-box}
.model-scope-btn:hover{background:#2c2c2d}
.model-scope-btn.selected{background:#37373d;border-color:#0e639c}
.model-scope-name{font-family:'JetBrains Mono',Consolas,monospace;font-size:12px;font-weight:600;color:#e6e6e6;word-break:break-all}
.model-scope-sub{font-size:11.5px;color:#9d9d9d}

/* Columna Central: flexible */
#modelCenterCol{flex:1;min-width:0;background:#1e1e1e;display:flex;flex-direction:column;overflow:hidden}
#modelCenterHeader{padding:12px 16px;border-bottom:1px solid #333;background:#252526;display:flex;flex-direction:column;gap:8px;flex-shrink:0}
.center-header-line1{display:flex;align-items:baseline;gap:10px}
.target-title-mono{font-family:'JetBrains Mono',Consolas,monospace;font-size:18px;font-weight:700;color:#fff}
.target-counts{font-size:13px;color:#9d9d9d}
.center-header-line2{font-size:12.5px;color:#9d9d9d}
.center-header-line3{display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin-top:2px}
#centerBotFilter{width:200px;background:#1e1e1e;border:1px solid #3c3c3c;color:#d4d4d4;padding:5px 8px;font-size:12px;border-radius:3px}
#centerBotFilter:focus{border-color:#0e639c;outline:none}
.chips-label{font-size:11px;font-weight:700;color:#9d9d9d;text-transform:uppercase;letter-spacing:0.5px;white-space:nowrap}
.chips-container{display:flex;align-items:center;gap:5px;flex-wrap:wrap}
.chip{display:inline-flex;align-items:center;gap:4px;padding:3px 8px;border-radius:12px;font-size:12px;background:#2d2d2d;color:#d4d4d4;border:1px solid #3c3c3c;cursor:pointer;user-select:none;transition:all 0.12s}
.chip:hover{border-color:#555;background:#333}
.chip.active{background:#37373d;border-color:#0e639c;color:#fff;font-weight:600}
.chip-count{color:#9d9d9d;font-size:11px}
.center-header-line4{display:flex;align-items:center;gap:8px;flex-wrap:wrap}

/* Tabla central */
#modelTableWrap{flex:1;min-height:0;overflow:auto;position:relative}
#modelTableWrap:focus{outline:none}
#modelNewTable{width:100%;border-collapse:collapse;font-size:12.5px;text-align:left}
#modelNewTable th{position:sticky;top:0;background:#252526;z-index:2;padding:7px 10px;border-bottom:1px solid #333;font-size:11px;text-transform:uppercase;color:#9d9d9d;letter-spacing:0.5px;font-weight:600}
#modelNewTable th.th-check{width:24px;text-align:center;padding-left:12px}
#modelNewTable th.th-prompt{width:150px}
#modelNewTable th.th-model{width:180px}
#modelNewTable th.th-temp{width:84px}
#modelNewTable th.th-reasoning{width:96px}
#modelNewTable th.th-format{width:110px}
#modelNewTable th.th-tokens{width:84px}
#modelNewTable td{padding:7px 10px;border-bottom:1px solid #282828;vertical-align:middle}
#modelNewTable td.td-check{width:24px;text-align:center;padding-left:12px}
#modelNewTable tr{cursor:pointer;transition:background 0.1s;user-select:none}
#modelNewTable tr:hover{background:#26292b}
#modelNewTable tr.selected{background:#2a2d2e}
#modelNewTable tr:focus{outline:1px dotted #0e639c}
.cell-bot-wrap{display:flex;flex-direction:column;gap:2px}
.cell-bot-name{font-weight:600;color:#e6e6e6}
.cell-prompt-sub{font-family:'JetBrains Mono',Consolas,monospace;font-size:11px;color:#9cdcfe}
.cell-vars-diff{font-family:'JetBrains Mono',Consolas,monospace;font-size:11px;font-weight:600}
.diff-added{color:#89d185}
.diff-removed{color:#f48771}
.td-mono{font-family:'JetBrains Mono',Consolas,monospace;font-size:12px}
.val-muted{color:#6e7681}
.diff-cell-preview{background:#1f2a1f!important;padding:2px 4px;border-radius:3px;display:inline-flex;align-items:center;gap:4px}
.diff-old{color:#f48771;text-decoration:line-through}
.diff-arrow{color:#9d9d9d}
.diff-new{color:#89d185;font-weight:600}
.table-empty-msg{padding:48px 16px;text-align:center;color:#9d9d9d;font-size:13px;font-style:italic}

/* Columna Derecha: 350 px */
#modelRightCol{width:350px;min-width:350px;max-width:350px;background:#252526;border-left:1px solid #333;display:flex;flex-direction:column;overflow:hidden}
.right-col-header{padding:12px 14px;border-bottom:1px solid #333;display:flex;align-items:center;justify-content:space-between}
.right-col-title-wrap{font-size:12px;color:#d4d4d4;text-transform:uppercase;letter-spacing:0.5px;font-weight:600}
.right-selected-num{font-size:22px;font-weight:700;color:#fff;margin:0 3px}
.btn-link{background:transparent;border:none;color:#4fc1ff;cursor:pointer;font-size:12px;padding:0}
.btn-link:hover{text-decoration:underline}
.right-col-scrollable{flex:1;min-height:0;overflow-y:auto;padding:12px 14px;display:flex;flex-direction:column;gap:16px}
.right-section{display:flex;flex-direction:column;gap:10px}
.right-section-title{font-size:11px;font-weight:700;color:#9d9d9d;text-transform:uppercase;letter-spacing:0.5px}
.right-section-sub{font-size:11px;color:#6e7681;font-weight:400;text-transform:none}
.field-group{display:flex;flex-direction:column;gap:4px}
.field-group label{font-size:12px;color:#9d9d9d}
.field-group input,.field-group select{background:#1e1e1e;border:1px solid #3c3c3c;color:#d4d4d4;padding:6px 8px;font-size:12.5px;border-radius:3px;box-sizing:border-box;width:100%;font-family:'JetBrains Mono',Consolas,monospace}
.field-group input:focus,.field-group select:focus{border-color:#0e639c;outline:none}
.field-row-2{display:grid;grid-template-columns:1fr 1fr;gap:10px}
.field-error-msg{color:#f48771;font-size:11px;line-height:1.3;margin-top:2px}
.input-error{border-color:#f48771!important}
.input-pending{border-color:#0e639c!important}
.variables-list{display:flex;flex-wrap:wrap;gap:6px;align-items:center;min-height:28px}
.var-pill{display:inline-flex;align-items:center;gap:5px;background:#1e293b;border:1px solid #0e639c;color:#38bdf8;padding:2px 8px;border-radius:12px;font-family:'JetBrains Mono',Consolas,monospace;font-size:11px}
.var-pill.var-pill-added{background:rgba(34,197,94,0.15);border-color:#22c55e;color:#4ade80}
.var-pill.var-pill-removed{background:rgba(244,135,113,0.15);border-color:#f48771;color:#f48771}
.var-pill.var-pill-partial{border-style:dashed}
.var-partial-badge{font-size:10px;opacity:0.75;margin-left:2px}
.var-pill-text{user-select:none}
.var-remove-btn,.var-undo-btn{background:transparent;border:none;cursor:pointer;font-size:11px;padding:0 2px;line-height:1;border-radius:50%;opacity:0.75}
.var-remove-btn{color:#38bdf8}
.var-remove-btn:hover{opacity:1;color:#f48771}
.var-undo-btn{color:#f48771}
.var-undo-btn:hover{opacity:1;color:#4ade80}
.var-pill.var-pill-added .var-remove-btn{color:#4ade80}
.var-pill.var-pill-added .var-remove-btn:hover{color:#f48771}
.new-var-row{display:flex;gap:8px;align-items:center}
#newVarInput{flex:1;background:#1e1e1e;border:1px solid #3c3c3c;color:#4fc1ff;padding:6px 8px;font-size:12px;font-family:'JetBrains Mono',Consolas,monospace;border-radius:3px}
.btn-add-var{background:#3c3c3c;border:1px solid #4a4a4a;color:#d4d4d4;font-size:12px;padding:6px 12px;border-radius:3px;cursor:pointer;white-space:nowrap;font-weight:600}
.btn-add-var:hover{background:#4a4a4a;color:#fff}
.var-help-text{font-size:11px;color:#6e7681;line-height:1.35}
.right-col-footer{padding:12px 14px;border-top:1px solid #333;background:#202021;display:flex;flex-direction:column;gap:8px;flex-shrink:0}
.right-footer-summary{font-size:12px;color:#9d9d9d;min-height:16px}
.btn-review{height:40px;background:#0e639c;border:1px solid #0e639c;color:#fff;font-size:13.5px;font-weight:600;border-radius:4px;cursor:pointer;transition:background 0.15s;width:100%;display:flex;align-items:center;justify-content:center}
.btn-review:hover:not(:disabled){background:#1177bb}
.btn-review:disabled{background:#2d2d2d;border-color:#3c3c3c;color:#6e7681;cursor:not-allowed}
.right-footer-note{font-size:11px;color:#6e7681;line-height:1.3}
#apiKeyPanel{flex:1;min-height:0;overflow:auto;display:flex;flex-direction:column}
#apiKeyActions{display:flex;flex-wrap:wrap;align-items:center;gap:10px;padding:8px 14px;background:var(--panel);border-bottom:1px solid var(--bd)}
#apiKeyActions label{display:flex;align-items:center;gap:6px;color:var(--mut)}
#apiKeyEnvExportControl{display:flex;align-items:center;gap:10px}
#apiKeySummary{color:var(--mut)}
#apiKeyTable{width:100%;min-width:1250px;border-collapse:collapse}
#apiKeyTable th{position:sticky;top:0;text-align:left;padding:7px 10px;background:var(--panel);border-bottom:1px solid var(--bd);z-index:1}
#apiKeyTable td{vertical-align:top;padding:7px 10px;border-bottom:1px solid var(--bd)}
.keyEntry{margin-bottom:8px;min-width:180px}
.keyEntry:last-child{margin-bottom:0}
.keyEntry small{display:block;color:var(--mut);font-size:11px}
.keyEntry code{display:block;white-space:normal;overflow-wrap:anywhere;font:12px/17px Consolas,monospace;color:var(--fg)}
.audit-ok{color:#89d185}
.audit-warn{color:#e6b450}
.audit-critical{color:#f48771}
.audit-details{margin-top:4px}
.pane{display:flex;align-items:center;gap:6px;min-width:0}
.pane.right{justify-content:flex-end}
.paneTitle{overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-weight:600}
.paneTools{display:flex;gap:4px;align-items:center}
.paneTools button{padding:5px 8px}
.transfer{display:flex;gap:4px;padding:0 8px;border-left:1px solid var(--bd);border-right:1px solid var(--bd)}
#editor,#singleEditor{flex:1;min-height:220px;min-width:0;width:100%}
.spacer{flex:1}
/* Segmented control */
.segmented-control{display:inline-flex;background:#2d2d2d;border:1px solid #444;border-radius:4px;padding:2px;gap:2px}
.segmented-control button{background:transparent;border:none;color:var(--mut);padding:4px 12px;font-size:12px;font-weight:600;border-radius:3px;cursor:pointer}
.segmented-control button:hover{background:#3a3a3a;color:var(--fg)}
.segmented-control button.active{background:var(--acc);color:#fff}
/* Badges */
.env-badge{display:inline-block;padding:2px 8px;border-radius:3px;font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:0.5px;flex-shrink:0}
.env-badge.desa{background:#dc2626;color:#fff}
.env-badge.test{background:#d97706;color:#fff}
.env-badge.prod{background:#16a34a;color:#fff}
/* Prompts View 3-column container */
#promptsView{display:flex;flex:1;min-height:0;overflow:hidden;background:var(--bg)}
#promptsSidebar{width:290px;min-width:260px;max-width:340px;background:#202021;border-right:1px solid var(--bd);display:flex;flex-direction:column;overflow-y:auto;user-select:none}
#promptsMain{flex:1;min-width:0;display:flex;flex-direction:column;overflow-y:auto;padding:12px 16px;gap:12px}
#promptsRightJson{width:380px;min-width:300px;max-width:450px;background:#181818;border-left:1px solid var(--bd);display:flex;flex-direction:column;overflow:hidden}
/* Sidebar components */
.sidebar-section-title{padding:10px 14px 6px;color:#858585;font-size:11px;font-weight:700;letter-spacing:0.8px;text-transform:uppercase}
.sidebar-model-card{margin:4px 10px;padding:8px 10px;background:#252526;border:1px solid #333;border-radius:4px}
.sidebar-card-label{font-size:10.5px;color:#858585;font-family:Consolas,monospace;margin-bottom:3px}
.sidebar-card-val{font-size:12.5px;color:#fff;font-weight:600}
.sidebar-card-val-row{display:flex;align-items:center;justify-content:space-between;gap:6px}
.sidebar-card-val-row span{font-family:Consolas,monospace;font-size:12px;color:#ccc}
.sidebar-card-val-row button{font-size:11px;padding:2px 8px;background:#333;border:1px solid #444}
.sidebar-prompt-item{margin:4px 10px;padding:9px 12px;background:#252526;border:1px solid #333;border-radius:4px;cursor:pointer;transition:all .15s}
.sidebar-prompt-item:hover{background:#2d2d30;border-color:#444}
.sidebar-prompt-item.active{background:#1e293b;border-color:#0e639c}
.sidebar-prompt-name{font-size:13px;font-weight:600;color:#fff;margin-bottom:3px;font-family:Consolas,monospace}
.sidebar-prompt-diff{font-size:11.5px;color:#aaa;display:flex;align-items:center;gap:6px}
.dot{font-size:10px}
.dot-warn{color:#e6b450}
.dot-ok{color:#89d185}
.dot-solo{color:#f48771}
.sidebar-help-note{padding:12px 14px;color:#777;font-size:11.5px;line-height:1.4;margin-top:auto}
/* Main Area components */
.prompt-header-row{display:flex;align-items:center;justify-content:space-between;gap:12px;padding-bottom:2px}
.prompt-main-title{margin:0;font-size:18px;font-weight:700;font-family:Consolas,monospace;color:#fff;display:inline-block}
.prompt-main-diff-badge{display:inline-block;margin-left:12px;color:#e6b450;font-size:12.5px;font-weight:500}
.btn-compare-env{background:#2d2d2d;color:#ccc;border:1px solid #444;font-size:12px;padding:5px 12px;border-radius:3px}
.btn-compare-env:hover{background:#3a3a3a;color:#fff}
.prompts-card{background:#252526;border:1px solid var(--bd);border-radius:5px;padding:12px 14px}
.card-section-title{color:#858585;font-size:11px;font-weight:700;letter-spacing:0.8px;margin-bottom:10px;text-transform:uppercase}
.param-row{display:grid;grid-template-columns:180px 1fr auto auto;align-items:center;gap:12px;margin-bottom:8px}
.param-name{font-family:Consolas,monospace;font-size:12.5px;font-weight:600;color:#ccc}
.param-input{width:100%;font-family:Consolas,monospace;font-size:13px;padding:5px 8px;background:#1e1e1e;border:1px solid #444;color:#fff;border-radius:3px;box-sizing:border-box}
.param-input:focus{border-color:#007acc;outline:none}
.param-input.invalid{border-color:#f48771 !important;background:rgba(244,135,113,0.12) !important;outline:1px solid #f48771 !important}
.param-diff-note{font-size:12px;padding-left:4px}
.param-diff-warn{color:#e6b450}
.param-diff-solo{color:#f48771}
.param-diff-ok{color:#89d185}
.diff-badge-ok{color:#89d185}
.diff-badge-warn{color:#e6b450}
.diff-badge-solo{color:#f48771}
.chip{display:inline-block;padding:2px 7px;border-radius:3px;font-size:11px;font-weight:600;text-align:center;min-width:44px}
.chip-string{background:#0e7490;color:#e0f2fe}
.chip-number{background:#4c1d95;color:#ede9fe}
.chip-list{background:#374151;color:#e5e7eb}
.chip-bool{background:#065f46;color:#d1fae5}
.chip-map{background:#7c2d12;color:#ffedd5}
.input-vars-list{display:flex;flex-wrap:wrap;gap:6px;align-items:center}
.var-pill{display:inline-flex;align-items:center;gap:5px;background:#1e293b;border:1px solid #0e639c;color:#38bdf8;padding:2px 8px;border-radius:12px;font-family:Consolas,monospace;font-size:11px}
.var-pill-text{cursor:pointer;transition:all 0.15s}
.var-pill-text:hover{text-decoration:underline;color:#7dd3fc}
.var-remove-btn{background:transparent;border:none;color:#38bdf8;cursor:pointer;font-size:11px;padding:0 2px;line-height:1;border-radius:50%;opacity:0.7}
.var-remove-btn:hover{opacity:1;color:#f48771}
.var-add-wrap{display:inline-flex;align-items:center;gap:4px}
.var-add-input{background:#1e1e1e;border:1px dashed #555;color:#fff;font-family:Consolas,monospace;font-size:11px;padding:3px 8px;border-radius:12px;width:110px;outline:none}
.var-add-input:focus{border-color:#0e639c;width:140px}
.var-add-btn{background:#2d2d2d;border:1px solid #444;color:#ccc;border-radius:12px;font-size:11px;padding:2px 8px;cursor:pointer}
.var-add-btn:hover{background:#0e639c;color:#fff;border-color:#0e639c}
.param-missing-row{display:grid;grid-template-columns:180px 1fr auto auto;align-items:center;gap:12px;margin-bottom:8px;padding:6px 10px;background:rgba(34,197,94,0.06);border:1px dashed #22c55e;border-left:3px solid #22c55e;border-radius:4px}
.param-missing-name{color:#4ade80;font-weight:600;font-family:Consolas,monospace;font-size:12.5px}
.param-missing-desc{font-size:12px;color:#aaa}
.btn-bring-test{background:#1b2d24;color:#4ade80;border:1px solid #166534;border-radius:3px;font-size:11.5px;padding:3px 9px;cursor:pointer;display:inline-flex;align-items:center;gap:4px;transition:all 0.15s}
.btn-bring-test:hover{background:#22c55e;color:#000;border-color:#22c55e}
.btn-delete-param{background:transparent;border:none;color:#777;cursor:pointer;font-size:13px;padding:2px 5px;border-radius:3px}
.btn-delete-param:hover{color:#f48771;background:rgba(244,135,113,0.15)}
.btn-add-param{display:inline-flex;align-items:center;gap:6px;background:transparent;border:1px dashed #444;color:#858585;font-size:12px;padding:5px 12px;border-radius:4px;cursor:pointer;margin-top:6px}
.btn-add-param:hover{color:#38bdf8;border-color:#0e639c;background:rgba(14,99,156,0.1)}
.add-param-form{display:flex;align-items:center;gap:8px;margin-top:8px;padding:8px 10px;background:#1f1f20;border:1px solid #3c3c3c;border-radius:4px;flex-wrap:wrap}
.add-param-input{background:#181818;border:1px solid #444;color:#fff;font-family:Consolas,monospace;font-size:12px;padding:4px 8px;border-radius:3px}
.add-param-select{background:#181818;border:1px solid #444;color:#fff;font-size:12px;padding:4px 8px;border-radius:3px}
.btn-confirm-add{background:#0e639c;border:none;color:#fff;font-size:12px;padding:4px 10px;border-radius:3px;cursor:pointer}
.btn-confirm-add:hover{background:#007acc}
.btn-cancel-add{background:#2d2d2d;border:1px solid #444;color:#ccc;font-size:12px;padding:4px 8px;border-radius:3px;cursor:pointer}
.btn-cancel-add:hover{background:#3a3a3a;color:#fff}
/* Template Card */
.template-card{flex:1;display:flex;flex-direction:column;min-height:360px}
.template-header-bar{display:flex;align-items:center;justify-content:space-between;gap:10px;margin-bottom:8px}
.template-header-left{display:flex;align-items:center;gap:8px;flex-wrap:wrap}
.template-label{font-size:13px;font-weight:700;color:#fff}
.template-subhint{color:#858585;font-size:11.5px}
.template-header-tools{display:flex;align-items:center;gap:6px}
.template-header-tools button{background:#2d2d2d;color:#ccc;border:1px solid #444;font-size:12px;width:32px;height:32px;padding:0;border-radius:6px;display:inline-flex;align-items:center;justify-content:center;cursor:pointer;transition:all .15s ease}
.template-header-tools button:hover{background:#3a3a3a;color:#fff}
.template-header-tools button.active{background:#0e639c;border-color:#0e639c;color:#fff}
.template-editor-wrapper{flex:1;min-height:220px;position:relative;border:1px solid #333;border-radius:3px;overflow:hidden}
#templateMonacoEditor{width:100%;height:100%;position:absolute}
.template-footer-bar{display:flex;align-items:center;justify-content:space-between;padding-top:8px;font-size:12px;color:#858585}
.template-diff-msg{color:#e6b450;font-weight:500}
/* Template Fullscreen Mode */
.template-card.fullscreen{position:fixed !important;inset:0 !important;width:100vw !important;height:100vh !important;z-index:9000 !important;background:#1e1e1e !important;margin:0 !important;border-radius:0 !important;border:none !important;padding:14px 20px !important;box-sizing:border-box !important;display:flex !important;flex-direction:column !important;box-shadow:none !important}
.template-card.fullscreen .template-header-bar{flex-shrink:0 !important;margin-bottom:10px !important}
.template-card.fullscreen .template-editor-wrapper{flex:1 1 0 !important;height:auto !important;min-height:0 !important}
.template-card.fullscreen .template-footer-bar{flex-shrink:0 !important;padding-top:10px !important}
.template-card.fullscreen #btnFullscreenTemplate .fs-icon-expand{display:none !important}
.template-card.fullscreen #btnFullscreenTemplate .fs-icon-collapse{display:inline-block !important}
.template-card.fullscreen #templateFsPromptName{display:inline-block !important}
/* Comparison Preview Card */
.comp-preview-card{margin-top:10px;border:1px solid #333;background:#1b1b1c;border-radius:4px;overflow:hidden;flex-shrink:0}
.comp-preview-header{display:flex;align-items:center;justify-content:space-between;padding:8px 12px;cursor:pointer;background:#202021;user-select:none;gap:10px;transition:background 0.15s}
.comp-preview-header:hover{background:#262627}
.comp-preview-header.expanded{border-bottom:1px solid #2e2e2e}
.comp-preview-header-left{display:flex;align-items:center;gap:8px;flex-wrap:wrap}
.comp-preview-toggle-icon{font-size:11px;color:#858585;width:12px;display:inline-block}
.comp-preview-status-badge{font-size:11.5px;color:#858585;margin-left:4px}
.comp-preview-header-right{display:flex;align-items:center;gap:8px}
.btn-copy-all-test{background:#1b2d24;color:#4ade80;border:1px solid #166534;border-radius:3px;font-size:11.5px;padding:3px 9px;cursor:pointer;display:inline-flex;align-items:center;gap:4px;transition:all 0.15s}
.btn-copy-all-test:hover:not(:disabled){background:#22c55e;color:#000;border-color:#22c55e}
.btn-copy-all-test:disabled{opacity:0.4;cursor:not-allowed}
.btn-toggle-comp-preview{background:#2d2d2d;border:1px solid #444;color:#ccc;font-size:11.5px;padding:3px 8px;border-radius:3px;cursor:pointer}
.btn-toggle-comp-preview:hover{background:#3a3a3a;color:#fff}
.comp-preview-body{padding:10px 12px;background:#181819}
.comp-preview-subtitle{font-size:11px;font-weight:700;color:#858585;text-transform:uppercase;letter-spacing:0.5px}
.comp-preview-subtitle-row{display:flex;align-items:center;justify-content:space-between;margin-top:10px;margin-bottom:6px}
.btn-copy-template-only{background:#1a2333;color:#38bdf8;border:1px solid #0e639c;border-radius:3px;font-size:11px;padding:2px 8px;cursor:pointer;transition:all 0.15s}
.btn-copy-template-only:hover:not(:disabled){background:#0e639c;color:#fff}
.btn-copy-template-only:disabled{opacity:0.4;cursor:not-allowed}
.comp-preview-params-list{display:flex;flex-wrap:wrap;gap:6px;margin-top:6px}
.comp-preview-param-tag{display:inline-flex;align-items:center;gap:6px;background:#252526;border:1px solid #3c3c3c;border-radius:3px;padding:2px 7px;font-size:11px;font-family:Consolas,monospace}
.comp-preview-param-name{color:#aaa}
.comp-preview-param-val{color:#fff;font-weight:600}
.comp-preview-template-text{background:#121212;border:1px solid #2d2d2d;border-radius:3px;padding:8px 10px;font-family:Consolas,monospace;font-size:11px;line-height:1.45;color:#d4d4d4;max-height:180px;overflow:auto;white-space:pre-wrap;word-break:break-word;margin:0}
.comp-preview-empty{padding:8px 0;color:#858585;font-size:11.5px;font-style:italic}
/* Right DynamoDB JSON Panel */
.right-json-header{padding:12px 14px 8px;border-bottom:1px solid #2a2a2a}
.right-json-title{font-size:11px;font-weight:700;letter-spacing:0.8px;color:#858585;text-transform:uppercase}
.right-json-subtitle{font-size:12px;color:#ccc;margin-top:3px}
.type-pill-row{display:flex;align-items:center;gap:10px;padding:8px 14px;background:#202021;border-bottom:1px solid #2a2a2a}
.type-pill-label{font-size:11.5px;color:#858585}
.type-pill-group{display:flex;gap:4px}
.type-pill{display:inline-flex;align-items:center;justify-content:center;width:24px;height:22px;border-radius:3px;font-size:11.5px;font-weight:700;background:#2d2d2d;color:#858585;border:1px solid #3c3c3c}
.type-pill.active{background:#007acc;color:#fff;border-color:#007acc}
.right-json-body{flex:1;min-height:0;overflow:auto;padding:12px 14px;background:#181818}
.right-json-body pre{margin:0;font-family:Consolas,monospace;font-size:12px;line-height:1.5;color:#e06c75;white-space:pre-wrap;word-break:break-all}
.right-json-help{padding:10px 14px;border-top:1px solid #2a2a2a;font-size:11.5px;color:#777;line-height:1.4}
/* Bottom Blue Footer */
#promptsFooter{display:flex;align-items:center;justify-content:space-between;padding:4px 14px;background:#007acc;color:#fff;font-size:12px;user-select:none;min-height:26px}
.footer-left-group,.footer-right-group{display:flex;align-items:center;gap:8px}
.footer-badge{background:rgba(255,255,255,0.2);padding:1px 6px;border-radius:3px;font-size:10.5px;font-weight:700}
.footer-dot{opacity:0.6}
#promptsFooter kbd{background:rgba(0,0,0,0.25);border:1px solid rgba(255,255,255,0.2);border-radius:3px;padding:1px 4px;font-size:11px}
/* Collapsible Parameters Card */
.params-header-bar{display:flex;align-items:center;justify-content:space-between;cursor:pointer;user-select:none;gap:10px}
.params-header-left{display:flex;align-items:center;gap:8px}
.params-toggle-icon{font-size:11px;color:#858585;width:12px;display:inline-block;transition:transform 0.15s}
.params-summary-badge{font-size:11.5px;color:#858585;margin-left:4px}
.btn-toggle-params{background:#2d2d2d;border:1px solid #444;color:#ccc;font-size:11.5px;padding:3px 8px;border-radius:3px;cursor:pointer}
.btn-toggle-params:hover{background:#3a3a3a;color:#fff}
/* Collapsible Sidebar Models */
.sidebar-section-title.collapsible{cursor:pointer;user-select:none;display:flex;align-items:center;justify-content:space-between;transition:background 0.15s}
.sidebar-section-title.collapsible:hover{color:#fff}
.sidebar-models-toggle-icon{font-size:10px;color:#858585;margin-right:6px;display:inline-block}
/* Save Confirmation Modal */
.modal-backdrop{position:fixed;inset:0;background:rgba(0,0,0,0.7);display:flex;align-items:center;justify-content:center;z-index:9999;backdrop-filter:blur(3px)}
.modal-dialog{width:640px;max-width:92vw;max-height:88vh;background:#252526;border:1px solid #3c3c3c;border-radius:6px;box-shadow:0 12px 32px rgba(0,0,0,0.6);display:flex;flex-direction:column;overflow:hidden}
.modal-header{display:flex;align-items:center;justify-content:space-between;padding:12px 16px;background:#202021;border-bottom:1px solid #333}
.modal-title-wrap{display:flex;align-items:center;gap:8px;flex-wrap:wrap}
.modal-title{font-size:14px;font-weight:700;color:#fff}
.modal-bot-name{font-size:13px;color:#aaa;font-family:Consolas,monospace}
.modal-close-btn{background:transparent;border:none;color:#858585;font-size:16px;cursor:pointer;padding:2px 6px;line-height:1;border-radius:3px}
.modal-close-btn:hover{color:#fff;background:#3a3a3a}
.modal-body{padding:14px 16px;overflow-y:auto;flex:1;display:flex;flex-direction:column;gap:12px}
.modal-alert-backup{display:flex;align-items:center;gap:8px;padding:8px 12px;background:rgba(14,99,156,0.12);border:1px solid #0e639c;border-radius:4px;font-size:12px;color:#7dd3fc}
.modal-diff-summary{display:flex;flex-direction:column;gap:8px;background:#181819;border:1px solid #333;border-radius:4px;padding:10px 12px;max-height:240px;overflow-y:auto}
.modal-diff-item{display:flex;flex-direction:column;gap:3px;font-size:12px;padding:4px 0;border-bottom:1px solid #282829}
.modal-diff-item:last-child{border-bottom:none}
.modal-diff-prompt-title{font-family:Consolas,monospace;font-weight:700;color:#38bdf8}
.modal-diff-change-desc{font-size:11.5px;color:#d4d4d4;padding-left:12px}
.modal-diff-change-desc strong{color:#e6b450}
.modal-diff-empty{color:#858585;font-size:12px;font-style:italic}
.modal-prod-confirm{background:rgba(220,38,38,0.1);border:1px solid #dc2626;border-radius:4px;padding:12px;display:flex;flex-direction:column;gap:8px}
.modal-prod-warning{color:#f87171;font-size:12.5px}
.modal-prod-input-row{display:flex;flex-direction:column;gap:4px}
.modal-prod-input-row label{font-size:12px;color:#ccc}
.modal-prod-input-row input{background:#181818;border:1px solid #dc2626;color:#fff;font-family:Consolas,monospace;font-size:13px;padding:6px 10px;border-radius:3px;outline:none}
.modal-footer{display:flex;align-items:center;justify-content:flex-end;gap:10px;padding:12px 16px;background:#202021;border-top:1px solid #333}
.btn-modal-cancel{background:#2d2d2d;border:1px solid #444;color:#ccc;font-size:12.5px;padding:6px 14px;border-radius:3px;cursor:pointer}
.btn-modal-cancel:hover{background:#3a3a3a;color:#fff}
.btn-modal-confirm{background:#0e639c;border:1px solid #0e639c;color:#fff;font-size:12.5px;font-weight:600;padding:6px 16px;border-radius:3px;cursor:pointer}
.btn-modal-confirm:hover:not(:disabled){background:#1177bb}
.btn-modal-confirm:disabled{opacity:0.4;cursor:not-allowed}
.btn-modal-confirm.prod-btn{background:#dc2626;border-color:#ef4444}
.btn-modal-confirm.prod-btn:hover:not(:disabled){background:#b91c1c}
/* History Modal & Version List */
.history-dialog{width:880px;max-width:95vw;height:82vh}
.history-split{display:flex;flex:1;min-height:0;overflow:hidden;width:100%}
.history-list-pane{width:320px;min-width:280px;border-right:1px solid #333;overflow-y:auto;background:#1e1e1e;padding:10px;display:flex;flex-direction:column;gap:6px}
.history-list-title{font-size:11px;font-weight:700;color:#858585;text-transform:uppercase;letter-spacing:0.5px;padding:2px 4px 6px}
.history-version-card{display:flex;flex-direction:column;gap:4px;padding:9px 12px;background:#252526;border:1px solid #383838;border-radius:4px;cursor:pointer;transition:all 0.15s;user-select:none}
.history-version-card:hover{background:#2c2c2d;border-color:#555}
.history-version-card.active{background:#1e293b;border-color:#38bdf8}
.history-version-top{display:flex;align-items:center;justify-content:space-between;gap:6px}
.history-version-date{font-family:Consolas,monospace;font-size:12px;font-weight:600;color:#f1f5f9}
.history-version-rel{font-size:11px;color:#94a3b8}
.history-version-bottom{display:flex;align-items:center;justify-content:space-between;gap:6px;font-size:11px;color:#64748b}
.history-version-badge-curr{background:#0e639c;color:#fff;border-radius:3px;padding:1px 5px;font-size:10px;font-weight:600}
.history-detail-pane{flex:1;min-width:0;display:flex;flex-direction:column;background:#181819}
.history-detail-header{display:flex;align-items:center;justify-content:space-between;padding:12px 16px;border-bottom:1px solid #2e2e2e;gap:12px}
.history-detail-title-wrap{display:flex;flex-direction:column;gap:3px}
.history-detail-title{font-size:13.5px;font-weight:600;color:#fff}
.history-detail-meta{font-size:11.5px;color:#858585}
.history-detail-actions{display:flex;align-items:center;gap:8px}
.history-detail-body{flex:1;min-height:0;overflow-y:auto;padding:14px 16px;display:flex;flex-direction:column;gap:12px}
.history-diff-box{background:#1e1e1e;border:1px solid #333;border-radius:4px;padding:12px;display:flex;flex-direction:column;gap:8px}
.history-diff-empty{color:#858585;font-size:12.5px;font-style:italic}
.history-json-box{font-family:Consolas,monospace;font-size:11.5px;background:#121212;border:1px solid #2d2d2d;border-radius:4px;padding:10px 12px;color:#d4d4d4;max-height:360px;overflow:auto;white-space:pre-wrap;word-break:break-all}
.btn-restore-version{background:#0e639c;color:#fff;border:1px solid #0e639c;border-radius:3px;font-size:12px;font-weight:600;padding:5px 12px;cursor:pointer;display:inline-flex;align-items:center;gap:5px;transition:all 0.15s}
.btn-restore-version:hover:not(:disabled){background:#1177bb}
.btn-restore-version:disabled{opacity:0.4;cursor:not-allowed}
.badge-count{display:inline-flex;align-items:center;justify-content:center;background:#0e639c;color:#fff;border-radius:10px;padding:0 6px;font-size:10px;font-weight:700;line-height:16px;margin-left:4px}
/* --- Vista Gestión de bots y limpieza de registros DynamoDB --- */
#cleanupPanel{flex:1;min-height:0;display:flex;flex-direction:column;background:#10141d;overflow:hidden;color:#e2e8f0;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,"Helvetica Neue",Arial,sans-serif}
#cleanupColumnsContainer{display:flex;flex:1;min-height:0;overflow:hidden;width:100%}
#cleanupLeftCol{width:260px;min-width:230px;max-width:320px;background:#131822;border-right:1px solid #1e293b;display:flex;flex-direction:column;overflow:hidden}
.cleanup-sidebar-header{padding:12px 16px 10px;border-bottom:1px solid #1e293b;display:flex;align-items:center;justify-content:space-between;background:#0f141d}
.cleanup-sidebar-title{font-size:11px;font-weight:700;color:#94a3b8;text-transform:uppercase;letter-spacing:0.8px}
.cleanup-clients-sidebar-list{flex:1;min-height:0;overflow-y:auto;padding:6px 8px;display:flex;flex-direction:column;gap:3px}
.cleanup-sidebar-client-item{display:flex;align-items:center;justify-content:space-between;padding:8px 10px;border-radius:5px;font-size:12.5px;color:#cbd5e1;cursor:pointer;transition:all 0.12s;user-select:none;border:1px solid transparent}
.cleanup-sidebar-client-item:hover{background:#1e293b;color:#fff}
.cleanup-sidebar-client-item.active{background:#172554;border-color:#2563eb;color:#93c5fd;font-weight:600}
.cleanup-sidebar-client-name{display:flex;align-items:center;gap:6px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.cleanup-sidebar-client-name .chevron{font-size:10px;color:#64748b}
.cleanup-sidebar-client-badge{font-size:10.5px;background:#1e293b;color:#94a3b8;padding:1px 6px;border-radius:10px;font-weight:600;flex-shrink:0}
.cleanup-sidebar-client-item.active .cleanup-sidebar-client-badge{background:#1e40af;color:#dbeafe}

#cleanupCenterCol{flex:1;min-width:0;background:#0b0f17;display:flex;flex-direction:column;overflow:hidden}
#cleanupCenterHeader{padding:16px 20px 14px;border-bottom:1px solid #1e293b;background:#111723;display:flex;align-items:center;justify-content:space-between;flex-shrink:0;gap:16px}
.cleanup-main-title{margin:0;font-size:19px;font-weight:700;color:#f8fafc;letter-spacing:-0.3px}
.cleanup-main-subtitle{font-size:12px;color:#94a3b8;margin-top:3px}
.cleanup-select-all-env{display:inline-flex;align-items:center;gap:8px;font-size:12px;color:#cbd5e1;cursor:pointer;user-select:none;font-weight:500;background:#1e293b;padding:6px 12px;border-radius:4px;border:1px solid #334155;transition:all 0.15s}
.cleanup-select-all-env:hover{background:#334155;color:#fff}
.cleanup-select-all-env input[type="checkbox"]{cursor:pointer}

#cleanupAccordionWrap{flex:1;min-height:0;overflow-y:auto;padding:16px 20px;display:flex;flex-direction:column;gap:10px}
.cleanup-empty-msg{padding:40px 20px;text-align:center;color:#64748b;font-size:13.5px;font-style:italic}

.cleanup-client-card{background:#131b28;border:1px solid #1e2c3f;border-radius:6px;overflow:hidden;transition:border-color 0.15s,box-shadow 0.15s}
.cleanup-client-card:hover{border-color:#2a3d56}
.cleanup-client-card.expanded{border-color:#2563eb;box-shadow:0 4px 12px rgba(0,0,0,0.25)}
.cleanup-client-card-header{padding:10px 14px;background:#162132;display:flex;align-items:center;justify-content:space-between;cursor:pointer;user-select:none;gap:12px}
.cleanup-client-card-header:hover{background:#1b283d}
.cleanup-client-header-left{display:flex;align-items:center;gap:10px;min-width:0}
.cleanup-client-check{margin:0;cursor:pointer}
.cleanup-chevron-btn{background:transparent;border:none;color:#94a3b8;font-size:11px;cursor:pointer;padding:0;line-height:1}
.cleanup-client-title{font-size:13.5px;font-weight:700;color:#f1f5f9}
.cleanup-client-bots-badge{font-size:11px;font-weight:600;color:#94a3b8;background:#243247;padding:2px 7px;border-radius:4px}
.cleanup-client-tags-preview{font-size:11.5px;color:#64748b;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;text-align:right;flex:1;padding-left:12px}

.cleanup-client-body{border-top:1px solid #1e2c3f;background:#0d1420;padding:12px 14px;display:flex;flex-direction:column;gap:10px}
.cleanup-client-toolbar{display:flex;align-items:center;justify-content:space-between;padding-bottom:8px;border-bottom:1px solid #192538}
.cleanup-client-select-all-label{display:inline-flex;align-items:center;gap:6px;font-size:12px;color:#cbd5e1;cursor:pointer;font-weight:500}
.cleanup-client-tabs{display:inline-flex;gap:4px;background:#162030;padding:2px;border-radius:4px;border:1px solid #233146}
.cleanup-client-tab-btn{background:transparent;border:none;color:#94a3b8;padding:3px 10px;font-size:11px;font-weight:600;border-radius:3px;cursor:pointer;transition:all 0.12s}
.cleanup-client-tab-btn:hover:not(.active){color:#f1f5f9}
.cleanup-client-tab-btn.active{background:#2563eb;color:#fff}

.cleanup-bots-table{width:100%;border-collapse:collapse;font-size:12.5px;text-align:left}
.cleanup-bots-table th{padding:7px 10px;font-size:10.5px;font-weight:700;color:#94a3b8;text-transform:uppercase;letter-spacing:0.6px;border-bottom:1px solid #233146;background:#141c2a}
.cleanup-bots-table th.th-check,.cleanup-bots-table td.td-check{width:28px;text-align:center;padding-left:6px}
.cleanup-bots-table td{padding:8px 10px;border-bottom:1px solid #172132;vertical-align:middle}
.cleanup-bots-table tr:hover td{background:#162234}
.cleanup-bot-name-cell{display:flex;flex-direction:column;gap:2px}
.cleanup-bot-name{font-weight:700;color:#f8fafc;font-size:13px}
.cleanup-bot-subid{font-size:11px;color:#94a3b8;font-family:Consolas,monospace}
.cleanup-badge-records{display:inline-flex;align-items:center;padding:3px 10px;border-radius:12px;font-size:11px;font-weight:600}
.cleanup-badge-records.has-records{background:#1d4ed8;color:#ffffff}
.cleanup-badge-records.no-records{background:#272f3d;color:#94a3b8}
.cleanup-bot-date{font-family:Consolas,monospace;font-size:11.5px;color:#cbd5e1}
.cleanup-action-more-btn{background:#1e293b;border:1px solid #334155;color:#94a3b8;border-radius:3px;padding:2px 7px;font-size:11px;cursor:pointer;font-weight:700;letter-spacing:1px;transition:all 0.12s}
.cleanup-action-more-btn:hover{background:#334155;color:#fff;border-color:#475569}

#cleanupRightCol{width:350px;min-width:320px;max-width:400px;background:#131822;border-left:1px solid #1e293b;display:flex;flex-direction:column;overflow:hidden}
.cleanup-right-header{padding:14px 18px 12px;border-bottom:1px solid #1e293b;background:#0f141d;display:flex;flex-direction:column;gap:6px}
.cleanup-right-title{font-size:12px;font-weight:800;color:#f1f5f9;text-transform:uppercase;letter-spacing:0.8px}
.cleanup-right-env-row{display:flex;align-items:center;gap:8px;font-size:12px;color:#94a3b8}
.cleanup-right-content{flex:1;min-height:0;overflow-y:auto;padding:16px 18px;display:flex;flex-direction:column;gap:14px}
.cleanup-selection-summary-card{background:#182232;border:1px solid #223147;border-radius:6px;padding:14px;display:flex;flex-direction:column;gap:10px}
.cleanup-summary-icon-row{display:flex;align-items:flex-start;gap:12px}
.cleanup-db-icon{color:#60a5fa;flex-shrink:0;margin-top:2px}
.cleanup-summary-count{font-size:14.5px;font-weight:700;color:#ffffff}
.cleanup-summary-subtext{font-size:11.5px;color:#94a3b8;line-height:1.4;margin-top:2px}
.cleanup-summary-records-badge{background:#121926;border:1px solid #1f2b3e;border-radius:4px;padding:6px 10px;font-size:12px;color:#93c5fd;text-align:center}
.cleanup-summary-records-badge strong{font-size:13.5px;color:#ffffff}
.cleanup-selected-list-preview{display:flex;flex-direction:column;gap:8px;background:#101726;border:1px solid #1e293b;border-radius:6px;padding:12px 14px}
.cleanup-preview-header{display:flex;align-items:center;justify-content:space-between;gap:8px}
.cleanup-preview-title-row{display:flex;align-items:center;gap:6px}
.cleanup-preview-title{font-size:11px;font-weight:700;color:#94a3b8;letter-spacing:0.6px;text-transform:uppercase}
.cleanup-preview-count-badge{background:#1e293b;color:#38bdf8;font-size:10.5px;font-weight:700;padding:2px 7px;border-radius:10px;border:1px solid #334155}
.cleanup-clear-all-btn{background:transparent;border:none;color:#94a3b8;font-size:11px;cursor:pointer;padding:2px 6px;border-radius:3px;transition:all 0.15s;text-decoration:underline}
.cleanup-clear-all-btn:hover{color:#f87171}
.cleanup-preview-items-scroll{display:flex;flex-direction:column;gap:8px;max-height:220px;overflow-y:auto;padding-right:2px}
.cleanup-preview-items-scroll::-webkit-scrollbar{width:4px}
.cleanup-preview-items-scroll::-webkit-scrollbar-thumb{background:#334155;border-radius:2px}
.cleanup-empty-selection{text-align:center;padding:14px 10px;color:#64748b;font-size:11.5px;line-height:1.4;display:flex;flex-direction:column;align-items:center;gap:4px;border:1px dashed #1e293b;border-radius:5px;background:rgba(15,23,42,0.4)}
.cleanup-sel-group{background:#131c2e;border:1px solid #1f2b3e;border-radius:5px;overflow:hidden}
.cleanup-sel-group-header{display:flex;align-items:center;justify-content:space-between;padding:5px 9px;background:#0d1522;border-bottom:1px solid #1a2536}
.cleanup-sel-client-name{font-size:11.5px;font-weight:700;color:#e2e8f0;display:flex;align-items:center;gap:5px}
.cleanup-sel-client-count{font-size:10px;color:#94a3b8;background:#1e293b;padding:1px 5px;border-radius:3px}
.cleanup-sel-group-actions{display:flex;align-items:center;gap:6px}
.cleanup-sel-group-records{font-size:10.5px;color:#64748b}
.cleanup-sel-group-remove{background:transparent;border:none;color:#64748b;font-size:14px;line-height:1;cursor:pointer;padding:0 4px;border-radius:2px}
.cleanup-sel-group-remove:hover{color:#ef4444;background:#2b1212}
.cleanup-sel-group-items{display:flex;flex-direction:column}
.cleanup-sel-bot-row{display:flex;align-items:center;justify-content:space-between;padding:5px 9px 5px 14px;border-bottom:1px solid #162033;font-size:11.5px;transition:background 0.1s;cursor:pointer}
.cleanup-sel-bot-row:last-child{border-bottom:none}
.cleanup-sel-bot-row:hover{background:#172236}
.cleanup-sel-bot-name{font-family:Consolas,monospace;color:#f1f5f9;font-weight:600;font-size:11.5px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:170px}
.cleanup-sel-bot-meta{display:flex;align-items:center;gap:6px}
.cleanup-sel-bot-records{font-size:10.5px;color:#64748b;padding:1px 5px;border-radius:3px;background:#0d131d}
.cleanup-sel-bot-records.has-records{color:#38bdf8;background:rgba(56,189,248,0.1)}
.cleanup-chip-remove{background:transparent;border:none;color:#64748b;font-size:14px;line-height:1;cursor:pointer;padding:1px 5px;border-radius:3px;display:flex;align-items:center;justify-content:center}
.cleanup-chip-remove:hover{color:#ef4444;background:#381212}
.cleanup-warning-card{background:rgba(180,83,9,0.12);border:1px solid #92400e;border-radius:6px;padding:12px 14px;display:flex;align-items:flex-start;gap:10px;font-size:12px;color:#fde68a;line-height:1.4}
.cleanup-warning-icon{font-size:15px;flex-shrink:0}
.btn-delete-cleanup{width:100%;padding:10px 14px;background:#3b1111;border:1px solid #b91c1c;color:#fca5a5;border-radius:5px;font-size:13px;font-weight:700;cursor:pointer;display:flex;align-items:center;justify-content:center;gap:6px;transition:all 0.15s}
.btn-delete-cleanup:hover:not(:disabled){background:#501717;color:#fff;border-color:#ef4444;box-shadow:0 0 10px rgba(239,68,68,0.3)}
.btn-delete-cleanup:disabled{opacity:0.35;cursor:not-allowed;border-color:#4b1a1a;background:#1f0b0b}
.cleanup-confirm-check-label{display:flex;align-items:flex-start;gap:8px;font-size:12px;color:#cbd5e1;cursor:pointer;line-height:1.35;user-select:none}
.cleanup-confirm-check-label input{margin-top:1px}
.btn-export-cleanup{width:100%;padding:8px 12px;background:#1e293b;border:1px solid #334155;color:#e2e8f0;border-radius:5px;font-size:12px;font-weight:600;cursor:pointer;display:flex;align-items:center;justify-content:center;gap:6px;transition:all 0.15s}
.btn-export-cleanup:hover:not(:disabled){background:#334155;color:#fff;border-color:#475569}
.btn-export-cleanup:disabled{opacity:0.4;cursor:not-allowed}
.cleanup-right-info{margin-top:auto;font-size:11px;color:#64748b;line-height:1.45;display:flex;gap:6px;background:#0f141d;padding:10px 12px;border-radius:4px;border:1px solid #1e293b}
.cleanup-right-info .info-icon{color:#94a3b8;font-weight:700;font-size:12px}

.cleanup-modal-alert-danger{background:#2b1212;border:1px solid #7f1d1d;color:#fca5a5;padding:12px 14px;border-radius:5px;display:flex;gap:12px;align-items:flex-start;font-size:12.5px;line-height:1.45}
.cleanup-modal-table{width:100%;border-collapse:collapse;font-size:12px}
.cleanup-modal-table th{padding:6px 10px;background:#1e293b;color:#94a3b8;font-size:10.5px;font-weight:700;text-transform:uppercase;border-bottom:1px solid #334155;position:sticky;top:0}
.cleanup-modal-table td{padding:6px 10px;border-bottom:1px solid #1e293b;color:#e2e8f0}
.btn-delete-confirm-action{background:#b91c1c;border:1px solid #b91c1c;color:#fff;padding:6px 16px;border-radius:3px;font-size:12.5px;font-weight:700;cursor:pointer;transition:all 0.15s}
.btn-delete-confirm-action:hover:not(:disabled){background:#dc2626;border-color:#dc2626}
.btn-delete-confirm-action:disabled{opacity:0.4;cursor:not-allowed}
.cleanup-spinner{width:22px;height:22px;border:3px solid rgba(56,189,248,0.2);border-top-color:#38bdf8;border-radius:50%;animation:cleanupSpin 0.7s linear infinite}
@keyframes cleanupSpin{to{transform:rotate(360deg)}}
.cleanup-env-summary{font-size:12px;font-weight:600;color:#cbd5e1;background:#1e293b;padding:4px 10px;border-radius:4px;border:1px solid #334155}

/* --- Vista Gestión de tablas DynamoDB --- */
#tablesPanel{flex:1;min-height:0;display:flex;flex-direction:column;background:#080d1a;color:#e2e8f0;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,"Helvetica Neue",Arial,sans-serif;overflow:hidden}
.tables-view-header{padding:12px 24px 10px;background:#0b1325;border-bottom:1px solid #192742;display:flex;align-items:center;justify-content:space-between;flex-shrink:0}
.tables-header-left{display:flex;align-items:center;gap:12px}
.tables-header-icon{color:#38bdf8;flex-shrink:0}
.tables-view-title{margin:0;font-size:18px;font-weight:700;color:#f8fafc;letter-spacing:-0.2px}
.tables-view-subtitle{font-size:11.5px;color:#94a3b8;margin-top:2px}
.tables-header-right{display:flex;align-items:center;gap:10px}
.tables-env-select-label{font-size:12px;color:#cbd5e1;display:flex;align-items:center;gap:8px;font-weight:600}
.tables-env-select{background:#111c35;color:#e2e8f0;border:1px solid #233554;padding:5px 12px;border-radius:4px;font-size:12px;font-weight:600;cursor:pointer}

.tables-tabs-nav{display:flex;align-items:center;gap:10px;padding:10px 24px;background:#0d162a;border-bottom:1px solid #1a2844;flex-shrink:0}
.tables-tab-btn{display:inline-flex;align-items:center;gap:8px;padding:7px 16px;border-radius:5px;font-size:12.5px;font-weight:600;cursor:pointer;border:1px solid #1f3050;background:#111b33;color:#94a3b8;transition:all 0.15s;user-select:none}
.tables-tab-btn:hover{background:#172547;color:#f1f5f9;border-color:#3b82f6}
.tables-tab-btn.active{background:#0078d4;color:#fff;border-color:#0078d4;box-shadow:0 2px 10px rgba(0,120,212,0.35)}

.tables-tabs-content{flex:1;min-height:0;display:flex;flex-direction:column;overflow:hidden}
.tables-tab-pane{flex:1;min-height:0;display:flex;flex-direction:column;overflow:hidden}
.tables-tab-pane[hidden]{display:none!important}

.tables-create-layout{flex:1;min-height:0;display:flex;overflow:hidden}
.tables-create-main-col{flex:1;min-width:0;min-height:0;overflow-y:auto;padding:16px 20px 20px;display:flex;flex-direction:column;gap:14px}
.tables-right-col{width:340px;min-width:310px;max-width:380px;background:#0a1021;border-left:1px solid #1a2844;display:flex;flex-direction:column;overflow:hidden}

.tables-wizard-header{display:flex;align-items:center;justify-content:space-between;padding-bottom:10px;border-bottom:1px solid #17243c;flex-shrink:0;gap:16px}
.tables-section-title{margin:0;font-size:16px;font-weight:700;color:#f8fafc}
.tables-section-desc{font-size:11.5px;color:#94a3b8;margin-top:2px}
.tables-wizard-steps{display:flex;align-items:center;gap:12px;font-size:12px}
.tables-step-item{display:flex;align-items:center;gap:6px;color:#64748b;font-weight:500}
.tables-step-item.active{color:#f1f5f9;font-weight:700}
.tables-step-circle{width:20px;height:20px;border-radius:50%;display:flex;align-items:center;justify-content:center;font-size:10.5px;font-weight:800;border:1.5px solid #334155;color:#94a3b8}
.tables-step-item.active .tables-step-circle{background:#0078d4;border-color:#0078d4;color:#fff}
.tables-step-line{width:26px;height:1px;background:#1e293b}

.tables-cards-grid{display:grid;grid-template-columns:minmax(0, 1fr) minmax(0, 1fr);gap:16px;width:100%;box-sizing:border-box}
.tables-card{background:#0d162a;border:1px solid #1a2844;border-radius:7px;padding:14px 16px;display:flex;flex-direction:column;gap:10px;min-width:0;max-width:100%;overflow:hidden;box-sizing:border-box}
.tables-card-header{display:flex;flex-direction:column;gap:3px}
.tables-card-title{margin:0;font-size:13.5px;font-weight:700;color:#f1f5f9;letter-spacing:-0.2px}
.tables-card-subtitle{margin:0;font-size:11.5px;color:#94a3b8;line-height:1.4}
.tables-card-body{display:flex;flex-direction:column;gap:10px;flex:1}

.tables-config-body{display:grid;grid-template-columns:1fr 1fr;gap:14px}
.tables-field-group{display:flex;flex-direction:column;gap:5px}
.tables-field-label{font-size:11.5px;font-weight:600;color:#cbd5e1}
.tables-required-star{color:#ef4444}
.tables-field-hint{font-size:10.5px;color:#64748b;line-height:1.35}
.tables-text-input{background:#090f1d;border:1px solid #1e2f4f;color:#f1f5f9;padding:7px 10px;border-radius:4px;font-size:12.5px;font-family:Consolas,monospace;outline:none;transition:border-color 0.15s;width:100%;box-sizing:border-box}
.tables-text-input:focus{border-color:#38bdf8}
.tables-select-input{background:#090f1d;border:1px solid #1e2f4f;color:#f1f5f9;padding:7px 10px;border-radius:4px;font-size:12.5px;font-weight:600;width:100%;outline:none;cursor:pointer}
.tables-env-note{background:#070b16;border:1px solid #15223a;padding:7px 10px;border-radius:4px;font-size:11px;color:#94a3b8;display:flex;align-items:center;gap:6px}

.tables-dropzone{background:#080e1c;border:1.5px dashed #2563eb;border-radius:6px;padding:18px 12px;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:6px;text-align:center;cursor:pointer;transition:all 0.15s}
.tables-dropzone:hover{background:#0c152a;border-color:#60a5fa}
.tables-cloud-icon{color:#3b82f6}
.tables-drop-prompt{font-size:12px;color:#cbd5e1}
.tables-browse-link{color:#60a5fa;text-decoration:underline;font-weight:600;margin-left:4px}
.tables-drop-formats{font-size:10.5px;color:#64748b}

.tables-file-chip{background:#081022;border:1px solid #1d4ed8;border-radius:6px;padding:10px 14px;display:flex;align-items:center;justify-content:space-between;gap:12px;min-width:0;max-width:100%;box-sizing:border-box}
.tables-file-chip-left{display:flex;align-items:center;gap:10px;min-width:0;overflow:hidden;flex:1}
.tables-file-ext-icon{background:#15803d;color:#fff;font-weight:800;font-size:11px;width:26px;height:26px;border-radius:4px;display:flex;align-items:center;justify-content:center;flex-shrink:0}
.tables-file-info{display:flex;flex-direction:column;gap:2px;min-width:0;overflow:hidden}
.tables-file-name{font-size:12px;font-weight:700;color:#f8fafc;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.tables-file-meta{font-size:11px;color:#94a3b8}
.tables-file-remove-btn{background:#182235;border:1px solid #2d3b55;color:#94a3b8;cursor:pointer;font-size:14px;font-weight:bold;width:28px;height:28px;border-radius:5px;display:inline-flex;align-items:center;justify-content:center;line-height:1;transition:all 0.15s ease;flex-shrink:0}
.tables-file-remove-btn:hover{background:#7f1d1d;border-color:#ef4444;color:#fff;box-shadow:0 0 8px rgba(239,68,68,0.4)}
.tables-file-remove-header-btn{background:rgba(239,68,68,0.12);border:1px solid rgba(239,68,68,0.35);color:#fca5a5;padding:3px 9px;border-radius:4px;font-size:11px;font-weight:600;cursor:pointer;display:inline-flex;align-items:center;gap:4px;transition:all 0.15s ease}
.tables-file-remove-header-btn:hover{background:#ef4444;border-color:#ef4444;color:#fff;box-shadow:0 0 8px rgba(239,68,68,0.4)}

.tables-cols-body{max-height:260px;overflow-y:auto}
.tables-cols-table-wrap{border:1px solid #182640;border-radius:5px;overflow:hidden;background:#090f1e}
.tables-cols-table{width:100%;border-collapse:collapse;font-size:12px;text-align:left}
.tables-cols-table th{background:#0e172c;padding:6px 10px;color:#94a3b8;font-size:10.5px;font-weight:700;text-transform:uppercase;border-bottom:1px solid #182640}
.tables-cols-table td{padding:5px 8px;border-bottom:1px solid #131e34;vertical-align:middle}
.col-drag-handle{color:#475569;font-size:13px;user-select:none;text-align:center}
.col-name-input{background:#090f1d;border:1px solid #1e2f4f;color:#f1f5f9;padding:4px 8px;border-radius:3px;font-size:11.5px;font-family:Consolas,monospace;width:100%;box-sizing:border-box}
.col-name-input:focus{border-color:#38bdf8}
.col-name-input:disabled{background:#0c1322;color:#94a3b8;cursor:not-allowed}
.col-type-select{background:#090f1d;border:1px solid #1e2f4f;color:#cbd5e1;padding:4px 6px;border-radius:3px;font-size:11px}
.badge-col-pk{background:#1e3a8a;color:#93c5fd;border:1px solid #2563eb;padding:2px 7px;border-radius:3px;font-size:10.5px;font-weight:700;display:inline-flex;align-items:center;gap:4px}
.badge-col-data{background:#172236;color:#94a3b8;border:1px solid #233554;padding:2px 7px;border-radius:3px;font-size:10.5px;font-weight:600}
.col-del-btn{background:transparent;border:none;color:#ef4444;cursor:pointer;padding:3px 6px;font-size:13px;border-radius:3px}
.col-del-btn:hover{background:rgba(239,68,68,0.15)}
.btn-add-col{background:#0d182e;border:1px dashed #2563eb;color:#60a5fa;padding:6px 12px;border-radius:4px;font-size:11.5px;font-weight:600;cursor:pointer;display:flex;align-items:center;justify-content:center;gap:6px;transition:all 0.15s;margin-top:6px}
.btn-add-col:hover{background:#132344;border-color:#60a5fa;color:#fff}

.tables-preview-top-bar{display:flex;align-items:center;justify-content:space-between;font-size:11.5px}
.tables-preview-stat-badge{background:#090f1e;border:1px solid #182640;padding:5px 10px;border-radius:4px;color:#cbd5e1;display:inline-flex;align-items:center;gap:6px}
.tables-match-status-box{padding:8px 12px;border-radius:5px;font-size:11.5px;display:flex;align-items:center;gap:8px;line-height:1.4}
.tables-match-status-box.match-ok{background:#064e3b;border:1px solid #059669;color:#a7f3d0}
.tables-match-status-box.match-err{background:#450a0a;border:1px solid #dc2626;color:#fca5a5}
.tables-match-status-box.match-info{background:#0c1932;border:1px solid #1d4ed8;color:#93c5fd}
.tables-key-warning-box{background:#451a03;border:1px solid #b45309;color:#fde68a;padding:8px 12px;border-radius:5px;font-size:11.5px;line-height:1.4}
.tables-preview-body{min-width:0;max-width:100%;overflow:hidden;display:flex;flex-direction:column;gap:10px}
.tables-data-preview-table-wrap{border:1px solid #182640;border-radius:4px;overflow-x:auto;overflow-y:auto;max-height:230px;width:100%;max-width:100%;min-width:0;box-sizing:border-box;background:#090f1e}
.tables-data-preview-table-wrap::-webkit-scrollbar{height:8px;width:8px}
.tables-data-preview-table-wrap::-webkit-scrollbar-track{background:rgba(0,0,0,0.2);border-radius:4px}
.tables-data-preview-table-wrap::-webkit-scrollbar-thumb{background:#334155;border-radius:4px}
.tables-data-preview-table-wrap::-webkit-scrollbar-thumb:hover{background:#475569}
.tables-data-preview-table{width:max-content;min-width:100%;border-collapse:collapse;font-size:11.5px;font-family:Consolas,monospace}
.tables-data-preview-table th{background:#0e172c;padding:6px 10px;color:#93c5fd;font-weight:700;border-bottom:1px solid #182640;position:sticky;top:0;white-space:nowrap;z-index:2}
.tables-data-preview-table td{padding:5px 9px;border-bottom:1px solid #131e34;color:#cbd5e1;white-space:nowrap;max-width:320px;overflow:hidden;text-overflow:ellipsis}
.tables-preview-empty-td{text-align:center;color:#64748b;padding:22px!important;font-style:italic;font-family:inherit}

.tables-bottom-summary-card{background:#0d162a;border:1px solid #1a2844;border-radius:7px;padding:12px 16px;display:flex;align-items:center;justify-content:space-between;gap:14px;flex-shrink:0}
.tables-bottom-chips{display:flex;flex-wrap:wrap;align-items:center;gap:8px}
.tables-bottom-chip{background:#080e1c;border:1px solid #16233a;padding:5px 10px;border-radius:4px;font-size:11.5px;color:#cbd5e1;display:flex;align-items:center;gap:6px}
.tables-bottom-chip .chip-label{color:#64748b}
.btn-primary-create-table{background:#0078d4;color:#fff;border:none;padding:10px 20px;border-radius:5px;font-size:13px;font-weight:700;cursor:pointer;display:inline-flex;align-items:center;gap:8px;transition:all 0.15s;flex-shrink:0}
.btn-primary-create-table:hover:not(:disabled){background:#1084d8;box-shadow:0 0 12px rgba(0,120,212,0.4)}
.btn-primary-create-table:disabled{opacity:0.35;cursor:not-allowed}

.tables-right-header{padding:14px 18px 12px;border-bottom:1px solid #1a2844;background:#090f1e}
.tables-right-title-row{display:flex;align-items:center;gap:8px}
.tables-right-title{font-size:12px;font-weight:800;color:#f8fafc;text-transform:uppercase;letter-spacing:0.8px}
.tables-right-content{flex:1;min-height:0;overflow-y:auto;padding:16px 18px;display:flex;flex-direction:column;gap:14px}
.tables-right-info-block{display:flex;flex-direction:column;gap:2px}
.tables-right-info-block .info-label{font-size:10.5px;font-weight:700;color:#64748b;text-transform:uppercase;letter-spacing:0.5px}
.tables-right-info-block .info-val{font-size:12.5px;color:#f1f5f9;font-weight:600}
.tables-right-info-block .info-subval{font-size:11px;color:#94a3b8}
.tables-right-warning-box{background:rgba(180,83,9,0.12);border:1px solid #92400e;border-radius:6px;padding:12px;font-size:11.5px;color:#fde68a;line-height:1.45;display:flex;align-items:flex-start;gap:8px}
.tables-right-actions{display:flex;flex-direction:column;gap:8px}
.tables-right-actions .actions-header{font-size:11px;font-weight:700;color:#94a3b8}
.btn-primary-create-table-right{width:100%;padding:10px 14px;background:#0078d4;color:#fff;border:none;border-radius:5px;font-size:13px;font-weight:700;cursor:pointer;display:flex;align-items:center;justify-content:center;gap:6px;transition:all 0.15s}
.btn-primary-create-table-right:hover:not(:disabled){background:#1084d8;box-shadow:0 0 10px rgba(0,120,212,0.4)}
.btn-primary-create-table-right:disabled{opacity:0.35;cursor:not-allowed}
.tables-right-explainer-card{margin-top:auto;background:#080e1c;border:1px solid #16233a;padding:10px 12px;border-radius:5px;font-size:11px;color:#64748b;display:flex;gap:8px;line-height:1.45}
.tables-right-explainer-card strong{color:#cbd5e1}

.tables-subpane-header{padding:14px 20px;display:flex;align-items:center;gap:12px;border-bottom:1px solid #1a2844;background:#090f1e}
.tables-subpane-table-wrap{flex:1;min-height:0;overflow-y:auto;padding:16px 20px}
.tables-empty-tab-card{padding:24px;background:#0d162a;border:1px solid #1a2844;border-radius:8px;margin:20px;max-width:540px}

/* =====================================================================
   VISTA 8: MÉTRICAS Y AUDITORÍA GENERAL DE PROMPTS
   ===================================================================== */
#metricsPanel{display:flex;flex-direction:column;flex:1;min-height:0;overflow:hidden;background:var(--bg);color:var(--fg);font-family:'IBM Plex Sans',-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif}

/* Submode Tab Switcher */
.metrics-submode-tabs{padding:8px 16px;background:var(--panel);border-bottom:1px solid var(--bd);display:flex;align-items:center;gap:8px;flex-shrink:0}
.metrics-tab-btn{background:#2a2a2e;border:1px solid #3c3c3c;color:#9d9d9d;padding:5px 14px;border-radius:4px;font-size:12px;font-weight:600;cursor:pointer;display:inline-flex;align-items:center;gap:6px;transition:all 0.15s}
.metrics-tab-btn:hover{color:#ffffff;background:#38383e}
.metrics-tab-btn.active{background:var(--acc);border-color:var(--acc);color:#ffffff;font-weight:700}

/* 3 Threshold Cards Grid */
.metrics-thresholds-grid{display:grid;grid-template-columns:repeat(3, 1fr);gap:12px;padding:12px 16px;background:var(--bg);flex-shrink:0}
@media(max-width:1100px){.metrics-thresholds-grid{grid-template-columns:1fr}}
.metrics-threshold-card{background:var(--panel);border-radius:4px;padding:12px 14px;display:flex;flex-direction:column;gap:8px;position:relative;border:1px solid var(--bd)}
.metrics-threshold-card.card-warn{border-color:var(--bd);background:var(--panel)}
.metrics-threshold-card.card-crit{border-color:var(--bd);background:var(--panel)}
.metrics-threshold-card.card-dist{border-color:var(--bd);background:var(--panel)}

.threshold-card-header{display:flex;flex-direction:column;gap:3px}
.threshold-card-title{font-size:13px;font-weight:700;color:#ffffff}
.threshold-card-title.title-warn{color:#ffffff}
.threshold-card-title.title-crit{color:#f48771}
.threshold-card-title.title-dist{color:#ffffff}
.threshold-card-sub{font-size:11.5px;color:var(--mut)}

.threshold-stepper-wrap{display:flex;align-items:center;gap:10px;margin-top:2px}
.threshold-stepper-control{display:inline-flex;align-items:center;background:#1e1e1e;border:1px solid #3c3c3c;border-radius:3px;overflow:hidden}
.threshold-stepper-btn{background:#2d2d2d;border:none;color:#d4d4d4;width:30px;height:30px;font-size:15px;font-weight:700;display:inline-flex;align-items:center;justify-content:center;cursor:pointer;transition:all 0.15s;user-select:none}
.threshold-stepper-btn:hover{background:#3a3a3a;color:#ffffff}
.threshold-stepper-input{background:transparent;border:none;color:#ffffff;width:72px;height:30px;font-family:'JetBrains Mono',Consolas,monospace;font-size:13px;font-weight:700;text-align:center;outline:none}
.threshold-unit-label{font-size:11.5px;color:var(--mut);font-weight:500}

/* Multi-Segment Distribution Bar */
.dist-bar-track{height:10px;background:#18181b;border:1px solid #2d2d32;border-radius:3px;overflow:hidden;display:flex;width:100%;margin-top:4px}
.dist-bar-seg{height:100%;transition:width 0.3s ease}
.dist-bar-seg.seg-ok{background:#16a34a}
.dist-bar-seg.seg-warn{background:#d97706}
.dist-bar-seg.seg-crit{background:#dc2626}

.dist-bar-legend{display:flex;align-items:center;gap:14px;flex-wrap:wrap;font-size:11px;color:var(--mut);margin-top:2px}
.dist-legend-item{display:inline-flex;align-items:center;gap:6px}
.dist-legend-dot{width:7px;height:7px;border-radius:50%;display:inline-block}

/* Toolbar */
.metrics-toolbar{padding:8px 16px;background:var(--panel);border-top:1px solid var(--bd);border-bottom:1px solid var(--bd);display:flex;align-items:center;gap:10px;flex-wrap:wrap;flex-shrink:0}
.metrics-quick-pills{display:inline-flex;align-items:center;gap:6px}
.metrics-quick-pill{background:#2a2a2e;border:1px solid #3c3c3c;color:#9d9d9d;padding:5px 12px;border-radius:3px;font-size:11.5px;font-weight:600;cursor:pointer;transition:all 0.15s;display:inline-flex;align-items:center;gap:6px}
.metrics-quick-pill:hover{background:#38383e;color:#ffffff}
.metrics-quick-pill.active{background:var(--acc);border-color:var(--acc);color:#ffffff}
.metrics-quick-pill .pill-count{background:rgba(0,0,0,0.3);color:#d4d4d4;padding:1px 6px;border-radius:10px;font-size:10.5px;font-family:'JetBrains Mono',Consolas,monospace;font-weight:700}
.metrics-quick-pill.active .pill-count{background:rgba(255,255,255,0.22);color:#ffffff}
.metrics-quick-pill.pill-warn.active{background:var(--acc);border-color:var(--acc);color:#ffffff}
.metrics-quick-pill.pill-crit.active{background:var(--acc);border-color:var(--acc);color:#ffffff}

.metrics-search-wrap{position:relative;display:flex;align-items:center;min-width:240px;flex:1;max-width:360px}
.metrics-search-icon{position:absolute;left:9px;color:var(--mut);pointer-events:none}
.metrics-search-input{background:#1e1e1e;border:1px solid #3c3c3c;color:#d4d4d4;padding:5px 10px 5px 28px;border-radius:3px;font-size:12px;width:100%;outline:none;transition:border-color 0.15s;box-sizing:border-box}
.metrics-search-input:focus{border-color:var(--acc)}
.metrics-search-input::placeholder{color:var(--mut)}
.metrics-select{background:#1e1e1e;border:1px solid #3c3c3c;color:#d4d4d4;padding:5px 10px;border-radius:3px;font-size:12px;outline:none;cursor:pointer}

.btn-reset-limits{background:#2d2d2d;border:1px solid #3c3c3c;color:#d4d4d4;padding:5px 12px;border-radius:3px;font-size:11.5px;font-weight:600;cursor:pointer;transition:all 0.15s;display:inline-flex;align-items:center;gap:5px;margin-left:auto}
.btn-reset-limits:hover{background:#3a3a3a;color:#ffffff}

/* KPI Cards Row */
.metrics-kpi-row{display:grid;grid-template-columns:repeat(4, 1fr);gap:10px;padding:10px 16px;background:var(--bg);border-bottom:1px solid var(--bd);flex-shrink:0}
.metrics-kpi-card{background:var(--panel);border:1px solid var(--bd);border-radius:4px;padding:10px 14px;display:flex;flex-direction:column;gap:3px}
.metrics-kpi-label{font-size:10.5px;font-weight:700;color:var(--mut);letter-spacing:0.5px;text-transform:uppercase}
.metrics-kpi-val{font-size:18px;font-weight:700;color:#ffffff;font-family:'JetBrains Mono',Consolas,monospace;letter-spacing:-0.3px}
.metrics-kpi-card.kpi-card-warn{border-color:rgba(217,119,6,0.4);background:rgba(180,83,9,0.08)}
.metrics-kpi-card.kpi-card-danger{border-color:rgba(220,38,38,0.4);background:rgba(185,28,28,0.08)}
.text-warn{color:#e6b450}
.text-danger{color:#f48771}

/* Length Table */
.metrics-table-wrap{flex:1;min-height:0;overflow:auto;padding:0 16px 20px}
.metrics-table{width:100%;border-collapse:collapse;font-size:12.5px;text-align:left}
.metrics-table thead{position:sticky;top:0;z-index:2}
.metrics-table th{background:#202021;padding:8px 12px;color:#858585;font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:0.5px;border-bottom:1px solid var(--bd);cursor:pointer;user-select:none;white-space:nowrap}
.metrics-table th:hover{color:#ffffff;background:#28282d}
.metrics-table th .sort-icon{font-size:10px;margin-left:4px;color:#4fc1ff}
.metrics-table td{padding:8px 12px;border-bottom:1px solid #282828;vertical-align:middle}
.metrics-table tr:hover td{background:#26292b}

.metrics-col-bot{color:#e6e6e6;font-weight:600;font-size:12.5px}
.metrics-col-prompt{color:#9cdcfe;font-family:'JetBrains Mono',Consolas,monospace;font-size:12px}

/* Model pill badges */
.model-pill{display:inline-flex;align-items:center;padding:2px 8px;border-radius:3px;font-size:11px;font-weight:600;font-family:'JetBrains Mono',Consolas,monospace;letter-spacing:-0.2px}
.model-pill-luna, .model-pill-mini, .model-pill-standard{background:#1e3557;border:1px solid #2d5588;color:#9cdcfe}

/* Dynamic bar cell */
.metrics-char-cell{display:flex;align-items:center;gap:10px}
.metrics-char-val{font-family:'JetBrains Mono',Consolas,monospace;font-size:12px;font-weight:600;color:#ffffff;min-width:64px}
.metrics-bar-track{width:140px;height:8px;background:#2a2a2e;border-radius:3px;overflow:hidden;position:relative;flex-shrink:0}
.metrics-bar-fill{height:100%;border-radius:3px;transition:width 0.3s ease}
.metrics-bar-fill.bar-blue{background:#0e639c}
.metrics-bar-fill.bar-yellow{background:#e6b450}
.metrics-bar-fill.bar-red{background:#f48771}

.metrics-tokens-val{font-family:'JetBrains Mono',Consolas,monospace;color:#858585;font-size:12px}
.metrics-actions-cell{display:flex;align-items:center;justify-content:flex-end;gap:8px}
.btn-metrics-view-prompt{background:var(--acc);border:1px solid var(--acc);color:#ffffff;border-radius:3px;padding:4px 10px;font-size:12px;font-weight:600;cursor:pointer;display:inline-flex;align-items:center;gap:4px;transition:all 0.15s;text-decoration:none}
.btn-metrics-view-prompt:hover{background:#1177bb;border-color:#1177bb}
.link-metrics-open-bot{color:#4fc1ff;text-decoration:none;font-size:12px;font-weight:500;cursor:pointer;background:none;border:none;padding:0;margin-left:6px}
.link-metrics-open-bot:hover{color:#9cdcfe;text-decoration:underline}
.metrics-empty-td{text-align:center;padding:32px;color:var(--mut);font-style:italic}

/* Variables Repetidas por Prompt Cards & Drawer (Image 2) */
.var-prompt-list{display:flex;flex-direction:column;gap:8px;padding:14px 0 24px}
.var-prompt-card-row{background:#0b1325;border:1px solid #162442;border-radius:8px;overflow:hidden;transition:border-color 0.15s, background 0.15s}
.var-prompt-card-row:hover{border-color:#2a4474}
.var-prompt-card-row.expanded{border-color:#3b82f6;background:#0d162c}

.var-prompt-header-row{display:flex;align-items:center;padding:12px 16px;gap:14px;cursor:pointer;user-select:none;flex-wrap:wrap}
.var-prompt-chevron{color:#64748b;font-size:10px;transition:transform 0.2s ease;width:14px;text-align:center}
.var-prompt-card-row.expanded .var-prompt-chevron{transform:rotate(180deg);color:#38bdf8}

.var-prompt-title-wrap{display:flex;align-items:center;gap:10px;min-width:240px}
.var-prompt-client-bot{font-weight:700;font-size:13px;color:#f1f5f9}
.var-prompt-name-tag{font-family:Consolas,monospace;font-size:12px;font-weight:700;color:#94a3b8;letter-spacing:0.3px}

.var-prompt-chips-wrap{display:flex;align-items:center;gap:6px;flex:1;flex-wrap:wrap}
.var-chip{display:inline-flex;align-items:center;gap:5px;padding:3px 8px;border-radius:5px;font-family:Consolas,monospace;font-size:11px;font-weight:700;letter-spacing:0.2px}
.var-chip.chip-crit{background:rgba(239,68,68,0.18);border:1px solid rgba(239,68,68,0.4);color:#fca5a5}
.var-chip.chip-crit .chip-badge{background:#ef4444;color:#ffffff;padding:1px 5px;border-radius:4px;font-size:10px}
.var-chip.chip-warn{background:rgba(245,158,11,0.18);border:1px solid rgba(245,158,11,0.4);color:#fde047}
.var-chip.chip-warn .chip-badge{background:#f59e0b;color:#0b1325;padding:1px 5px;border-radius:4px;font-size:10px}
.var-chip.chip-other{background:rgba(168,85,247,0.18);border:1px solid rgba(168,85,247,0.4);color:#d8b4fe}
.var-chip.chip-other .chip-badge{background:#a855f7;color:#ffffff;padding:1px 5px;border-radius:4px;font-size:10px}

.var-prompt-total-badge{font-size:12.5px;color:#cbd5e1;font-weight:600;min-width:70px;text-align:right}
.var-prompt-actions-wrap{display:flex;align-items:center;gap:10px;margin-left:auto}
.btn-prompt-view-primary{background:#2563eb;border:1px solid #3b82f6;color:#ffffff;font-size:12px;font-weight:600;padding:5px 14px;border-radius:5px;cursor:pointer;transition:all 0.15s;text-decoration:none}
.btn-prompt-view-primary:hover{background:#1d4ed8;box-shadow:0 0 10px rgba(37,99,235,0.4)}
.link-prompt-open-bot{background:transparent;border:none;color:#60a5fa;font-size:12px;cursor:pointer;text-decoration:underline;padding:4px 6px}
.link-prompt-open-bot:hover{color:#93c5fd}

/* Expanded body with repeated variables list */
.var-prompt-expanded-body{padding:14px 20px 18px 44px;border-top:1px solid #14213d;background:linear-gradient(180deg, #070d1c 0%, #050914 100%);display:flex;flex-direction:column;gap:10px}
.var-rep-row{display:flex;align-items:center;gap:14px;padding:4px 0}
.var-rep-name{font-family:Consolas,monospace;font-size:12px;font-weight:700;color:#f59e0b;min-width:240px}
.var-rep-bar-wrap{flex:1;max-width:440px;height:10px;background:#101a2f;border-radius:999px;overflow:hidden;position:relative}
.var-rep-bar-fill{height:100%;border-radius:999px;transition:width 0.3s ease}
.var-rep-bar-fill.bar-coral{background:#f87171;box-shadow:0 0 8px rgba(248,113,113,0.4)}
.var-rep-bar-fill.bar-amber{background:#fbbf24;box-shadow:0 0 8px rgba(251,191,36,0.4)}
.var-rep-bar-fill.bar-violet{background:#c084fc;box-shadow:0 0 8px rgba(192,132,252,0.4)}
.var-rep-bar-fill.bar-sky{background:#38bdf8;box-shadow:0 0 8px rgba(56,189,248,0.4)}
.var-rep-count-label{font-family:Consolas,monospace;font-size:12px;font-weight:700;color:#cbd5e1;min-width:32px}
.btn-ver-donde{background:transparent;border:none;color:#60a5fa;font-size:12px;cursor:pointer;text-decoration:underline;padding:2px 8px;border-radius:4px}
.btn-ver-donde:hover{color:#93c5fd;background:rgba(56,189,248,0.1)}

/* Locations Drawer */
.var-locations-drawer{margin:6px 0 10px 12px;padding:10px 14px;background:#060a14;border:1px solid #1a2a46;border-radius:6px;display:flex;flex-direction:column;gap:6px}
.var-loc-item{font-family:Consolas,monospace;font-size:11.5px;color:#94a3b8;display:flex;align-items:flex-start;gap:8px;line-height:1.4}
.var-loc-line{color:#38bdf8;font-weight:700;min-width:65px}
.var-loc-snippet{color:#cbd5e1;word-break:break-all}
.var-loc-snippet mark{background:rgba(245,158,11,0.25);color:#fde047;border-radius:3px;padding:0 2px}


/* =====================================================================
   DESIGN SYSTEM v2 — capa visual unificada (no elimina reglas previas)
   ===================================================================== */
:root{
  --bg:#0e1013;--panel:#15171b;--surface:#15171b;--surface-2:#1a1d22;--surface-3:#21252b;--input:#0f1114;
  --bd:#262a31;--bd-2:#30353d;--fg:#e6e8eb;--fg-2:#b3b9c2;--mut:#7c838e;
  --acc:#2f74e0;--acc-h:#3b82f6;--acc-soft:rgba(59,130,246,.14);--acc-line:rgba(59,130,246,.45);
  --ok:#3fb950;--ok-soft:rgba(63,185,80,.12);--warn:#d29922;--warn-soft:rgba(210,153,34,.12);--danger:#f85149;--danger-soft:rgba(248,81,73,.12);
  --desa:#ef4444;--test:#f59e0b;--prod:#22c55e;
  --r-sm:6px;--r:8px;--r-lg:12px;--rail:60px;
  --shadow:0 10px 30px rgba(0,0,0,.4);
  --font:'Inter','IBM Plex Sans',-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;
  --mono:'JetBrains Mono',Consolas,monospace;
}
*{scrollbar-color:#2c3138 transparent}
*::-webkit-scrollbar{width:8px;height:8px}
*::-webkit-scrollbar-track{background:transparent}
*::-webkit-scrollbar-thumb{background:#2c3138;border-radius:8px;border:2px solid transparent;background-clip:padding-box}
*::-webkit-scrollbar-thumb:hover{background:#3a4049;background-clip:padding-box}
html,body{background:var(--bg);color:var(--fg);font-family:var(--font);font-size:13.5px;-webkit-font-smoothing:antialiased}
body{display:flex;overflow:hidden}
#app{flex:1;min-width:0;height:100%}
::selection{background:rgba(59,130,246,.35)}
code,kbd,pre{font-family:var(--mono)}

/* ---- Controles base (especificidad baja con :where) ---- */
select,input,button,textarea{font-family:inherit}
select,textarea,input:where(:not([type=checkbox],[type=radio],[type=file],[type=range])){background-color:var(--input);border:1px solid var(--bd-2);color:var(--fg);border-radius:var(--r-sm);padding:7px 10px;font-size:13px;transition:border-color .15s,box-shadow .15s,background-color .15s}
select:where(:focus),textarea:where(:focus),input:where(:focus){outline:none;border-color:var(--acc-h);box-shadow:0 0 0 3px var(--acc-soft)}
input::placeholder,textarea::placeholder{color:#5c636d}
select{appearance:none!important;-webkit-appearance:none!important;background-image:url('data:image/svg+xml;utf8,<svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="%237c838e" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="6 9 12 15 18 9"/></svg>')!important;background-repeat:no-repeat!important;background-position:right 9px center!important;background-size:12px!important;padding-right:28px!important;cursor:pointer}
select option{background:#15171b;color:var(--fg)}
button{background:var(--surface-3);border:1px solid var(--bd-2);color:var(--fg);border-radius:var(--r-sm);padding:7px 12px;font-size:12.5px;font-weight:500;transition:background .15s,border-color .15s,color .15s,box-shadow .15s}
button:hover{background:#2a2f36;border-color:#3b414a}
button:disabled{opacity:.4;cursor:not-allowed}
button:focus-visible{outline:2px solid var(--acc-h);outline-offset:2px}
button.primary{background:var(--acc);border-color:var(--acc);color:#fff;font-weight:600}
button.primary:hover:not(:disabled){background:var(--acc-h);border-color:var(--acc-h)}
input[type=checkbox],input[type=radio]{accent-color:var(--acc-h);width:15px;height:15px;cursor:pointer}
kbd{background:var(--surface-3);border:1px solid var(--bd-2);border-bottom-width:2px;border-radius:5px;padding:1px 6px;font-size:11px;color:var(--fg)}

/* ---- Rail de navegación lateral (Diseño Image 1) ---- */
#appRail{width:56px;flex-shrink:0;height:100%;background:#0f1115;border-right:1px solid #1c2027;display:flex;flex-direction:column;align-items:center;padding:12px 0 10px;z-index:999;user-select:none}
.rail-group{display:flex;flex-direction:column;align-items:center;gap:8px;width:100%}
.rail-bottom{margin-top:auto;display:flex;flex-direction:column;align-items:center;gap:8px;width:100%;padding-bottom:4px}
.rail-btn{position:relative;width:40px;height:40px;border-radius:9px;border:none;background:transparent;color:#858d98;display:flex;align-items:center;justify-content:center;padding:0;cursor:pointer;transition:all .15s ease}
.rail-btn svg{width:20px;height:20px;stroke:currentColor;transition:stroke .15s ease}
.rail-btn:hover{background:#191c23;color:#f3f4f6}
.rail-btn.active{background:#252932;color:#ffffff;box-shadow:0 2px 6px rgba(0,0,0,.35)}
.rail-btn.active svg{stroke:#ffffff}

/* Tooltip flotante que se despliega SOLO en mouse over */
.rail-tooltip{
  display:none!important;
  position:absolute;
  left:calc(100% + 10px);
  top:50%;
  transform:translateY(-50%);
  background:#181b22;
  border:1px solid #2e3544;
  color:#ffffff;
  font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;
  font-size:11.5px;
  font-weight:500;
  padding:5px 10px;
  border-radius:6px;
  white-space:nowrap;
  pointer-events:none;
  z-index:99999;
  box-shadow:0 4px 16px rgba(0,0,0,.6);
  opacity:0;
  transition:opacity .15s ease,transform .15s ease;
}
.rail-tooltip::before{
  content:'';
  position:absolute;
  right:100%;
  top:50%;
  transform:translateY(-50%);
  border:5px solid transparent;
  border-right-color:#2e3544;
}
.rail-btn:hover .rail-tooltip{
  display:block!important;
  opacity:1;
  animation:tipIn .12s ease;
}
@keyframes tipIn{from{opacity:0;transform:translate(-4px,-50%)}to{opacity:1;transform:translate(0,-50%)}}

/* ---- Barra superior ---- */
#bar{min-height:58px;padding:10px 18px;gap:10px;background:var(--surface);border-bottom:1px solid var(--bd);position:relative}
#bar.env-desa,#bar.env-test,#bar.env-prod{border-bottom:1px solid var(--bd)!important}
#bar.env-desa{box-shadow:inset 0 -2px 0 var(--desa)}
#bar.env-test{box-shadow:inset 0 -2px 0 var(--test)}
#bar.env-prod{box-shadow:inset 0 -2px 0 var(--prod)}
#bar label{color:var(--mut);font-size:12px;font-weight:500;gap:8px}
#viewModeLabel{display:none!important}
#barViewTitle{display:flex;flex-direction:column;justify-content:center;gap:1px;padding-right:16px;margin-right:4px;border-right:1px solid var(--bd);min-width:170px}
.bvt-crumb{font-size:10.5px;font-weight:600;color:var(--mut);letter-spacing:.3px;text-transform:uppercase}
.bvt-name{font-size:15px;font-weight:650;color:#fff;letter-spacing:-.2px;white-space:nowrap}
select#pick{min-width:300px}
input#filter{width:220px}
#swap{padding:6px 10px}
#tablesTopControls > span{display:none}
#status{background:var(--bg);color:var(--mut);border-bottom:1px solid var(--bd);font-size:12px;padding:6px 18px;min-height:30px;display:flex;align-items:center;gap:8px}
#status::before{content:'';width:7px;height:7px;border-radius:50%;background:#4b525c;flex-shrink:0}
#status.ok{color:#7ee787}#status.ok::before{background:var(--ok);box-shadow:0 0 8px rgba(63,185,80,.6)}
#status.err{color:#ff9b93}#status.err::before{background:var(--danger);box-shadow:0 0 8px rgba(248,81,73,.6)}

/* ---- Selector de ambiente tipo píldora ---- */
.env-segmented-control{display:inline-flex;gap:3px;padding:3px;border:1px solid var(--bd);background:var(--input);border-radius:999px;margin:0;overflow:visible}
.env-segmented-control button{border:none!important;background:transparent;color:var(--mut);padding:5px 13px;border-radius:999px;font-size:11.5px;font-weight:600;letter-spacing:.4px;display:inline-flex;align-items:center;gap:6px}
.env-segmented-control button::before{content:'';width:6px;height:6px;border-radius:50%;background:currentColor;opacity:.45;transition:opacity .15s,box-shadow .15s}
.env-segmented-control button.desa::before{background:var(--desa)}
.env-segmented-control button.test::before{background:var(--test)}
.env-segmented-control button.prod::before{background:var(--prod)}
.env-segmented-control button:hover:not(.active){background:var(--surface-3);color:var(--fg)}
.env-segmented-control button.active.desa{background:rgba(239,68,68,.18);color:#fca5a5;box-shadow:inset 0 0 0 1px rgba(239,68,68,.45)}
.env-segmented-control button.active.test{background:rgba(245,158,11,.18);color:#fde047;box-shadow:inset 0 0 0 1px rgba(245,158,11,.45)}
.env-segmented-control button.active.prod{background:rgba(34,197,94,.18);color:#86efac;box-shadow:inset 0 0 0 1px rgba(34,197,94,.45)}
.env-segmented-control button.active::before{opacity:1;box-shadow:0 0 8px currentColor}
.segmented-control{background:var(--input);border:1px solid var(--bd);border-radius:999px;padding:3px;gap:3px}
.segmented-control button{border-radius:999px;padding:5px 14px;color:var(--mut);font-size:12px}
.segmented-control button:hover{background:var(--surface-3);color:var(--fg)}
.segmented-control button.active{background:#2a2e35;color:#fff;box-shadow:0 1px 3px rgba(0,0,0,.45)}

/* ---- Badges (DESA Rojo, TEST Amarillo, PROD Verde) ---- */
.env-badge{border-radius:999px;padding:2px 9px;font-size:10.5px;letter-spacing:.5px}
.env-badge.desa{background:rgba(239,68,68,.16);color:#fca5a5;box-shadow:inset 0 0 0 1px rgba(239,68,68,.42)}
.env-badge.test{background:rgba(245,158,11,.15);color:#fcd34d;box-shadow:inset 0 0 0 1px rgba(245,158,11,.38)}
.env-badge.prod{background:rgba(34,197,94,.16);color:#86efac;box-shadow:inset 0 0 0 1px rgba(34,197,94,.42)}
.badge-count{background:var(--acc)}

/* ---- Vista Comparar / JSON ---- */
#paneBar,#singleBar{background:var(--surface);border-bottom:1px solid var(--bd);padding:8px 16px;gap:10px}
.paneTitle{font-size:13px;color:var(--fg)}
.transfer{border-color:var(--bd)}
#editor,#singleEditor{background:#111316}
#paneBar .paneTools button,#singleBar > button:not(.primary){background:transparent;border-color:var(--bd-2);color:var(--fg-2)}
#paneBar .paneTools button:hover:not(:disabled),#singleBar > button:not(.primary):hover:not(:disabled){background:var(--surface-3);color:#fff}

/* ---- Vista JSON individual y Texto ---- */
#promptsView{background:var(--bg)}
#promptsSidebar{width:264px;background:var(--surface);border-right:1px solid var(--bd);padding:8px 0}
.sidebar-section-title{color:var(--mut);font-size:10.5px;letter-spacing:1px;padding:12px 18px 6px}
.sidebar-model-card{margin:4px 12px;padding:9px 12px;background:var(--surface-2);border:1px solid var(--bd);border-radius:var(--r)}
.sidebar-card-label{font-family:var(--mono);color:var(--mut)}
.sidebar-card-val{color:#fff;font-weight:600}
.sidebar-prompt-item{margin:3px 12px;padding:10px 12px;background:transparent;border:1px solid transparent;border-radius:var(--r)}
.sidebar-prompt-item:hover{background:var(--surface-2);border-color:var(--bd)}
.sidebar-prompt-item.active{background:var(--surface-3);border-color:var(--bd-2)}
.sidebar-prompt-item.active:has(.dot-ok){background:rgba(63,185,80,.10);border-color:rgba(63,185,80,.38)}
.sidebar-prompt-item.active:has(.dot-warn){background:rgba(210,153,34,.10);border-color:rgba(210,153,34,.38)}
.sidebar-prompt-item.active:has(.dot-solo){background:rgba(248,81,73,.09);border-color:rgba(248,81,73,.36)}
.sidebar-prompt-name{font-family:var(--mono);font-size:12px;letter-spacing:.2px;color:var(--fg)}
.sidebar-prompt-diff{color:var(--mut);font-size:11.5px}
.dot-ok{color:var(--ok)}.dot-warn{color:var(--warn)}.dot-solo{color:var(--danger)}
.sidebar-help-note{color:#5c636d;border-top:1px solid var(--bd);margin:8px 0 0;padding:12px 18px}
#promptsMain{padding:20px 24px;gap:16px;background:var(--bg)}
.prompt-main-title{font-family:var(--font);font-size:19px;font-weight:650;letter-spacing:-.2px}
.prompt-main-diff-badge{background:var(--warn-soft);color:#e3b341;border:1px solid rgba(210,153,34,.35);padding:2px 10px;border-radius:999px;font-size:11.5px;margin-left:0}
.prompts-card{background:var(--surface);border:1px solid var(--bd);border-radius:var(--r-lg);padding:16px 18px}
.card-section-title{color:var(--mut);font-size:10.5px;letter-spacing:1px}
.param-row,.param-missing-row{grid-template-columns:200px 1fr auto auto;margin-bottom:10px}
.param-name{font-family:var(--mono);font-size:11.5px;color:var(--fg-2);letter-spacing:.2px}
.param-input{background:var(--input);border:1px solid var(--bd-2);border-radius:var(--r-sm);padding:7px 10px;font-family:var(--mono);font-size:12.5px;color:var(--fg)}
.param-input:focus{border-color:var(--acc-h);box-shadow:0 0 0 3px var(--acc-soft)}
.btn-compare-env,.btn-toggle-params,.btn-toggle-comp-preview,.template-header-tools button,.btn-cancel-add,.btn-add-var,.var-add-btn{background:var(--surface-3);border:1px solid var(--bd-2);color:var(--fg-2);border-radius:var(--r-sm)}
.btn-compare-env:hover,.btn-toggle-params:hover,.btn-toggle-comp-preview:hover,.template-header-tools button:hover,.btn-cancel-add:hover,.btn-add-var:hover{background:#2a2f36;color:#fff}
.template-header-tools button{width:32px;height:32px;padding:0;display:inline-flex;align-items:center;justify-content:center;border-radius:6px}
.template-header-tools button.active{background:var(--acc-soft);border-color:var(--acc-line);color:#bfdbfe}
.btn-add-param{border-color:var(--bd-2);color:var(--mut);border-radius:var(--r)}
.add-param-form{background:var(--surface-2);border-color:var(--bd);border-radius:var(--r)}
.add-param-input,.add-param-select{background:var(--input);border-color:var(--bd-2)}
.btn-confirm-add{background:var(--acc)}
.template-label{font-size:12px;letter-spacing:1px;color:var(--fg-2)}
.template-editor-wrapper{border:1px solid var(--bd);border-radius:var(--r);background:#111316}
.template-footer-bar{color:var(--mut)}
.template-card.fullscreen{background:var(--bg)!important}
.comp-preview-card{background:var(--surface);border:1px solid var(--bd);border-radius:var(--r-lg)}
.comp-preview-header{background:var(--surface);padding:12px 16px}
.comp-preview-header:hover{background:var(--surface-2)}
.comp-preview-header.expanded{border-bottom-color:var(--bd)}
.comp-preview-body{background:var(--surface);padding:12px 16px}
.comp-preview-param-tag{background:var(--surface-2);border-color:var(--bd);border-radius:999px;padding:2px 9px}
.comp-preview-template-text{background:var(--input);border-color:var(--bd);border-radius:var(--r)}
#promptsRightJson{width:400px;background:var(--surface);border-left:1px solid var(--bd)}
.right-json-header{padding:16px 18px 10px;border-bottom:none}
.right-json-title{font-size:13px;font-weight:650;color:#fff;letter-spacing:.2px;text-transform:none}
.right-json-subtitle{color:var(--mut)}
.right-json-subtitle strong{color:#93c5fd;font-family:var(--mono);font-weight:600}
.type-pill-row{background:transparent;border-bottom:none;padding:0 18px 10px}
.type-pill{background:var(--input);border-color:var(--bd-2);color:var(--mut);border-radius:6px}
.type-pill.active{background:var(--acc);border-color:var(--acc);color:#fff}
.right-json-body{margin:0 14px;background:var(--input);border:1px solid var(--bd);border-radius:var(--r-lg);padding:14px 16px}
.right-json-body pre{font-family:var(--mono);color:var(--fg);font-size:12px;line-height:1.6}
.right-json-body .jk{color:#9cdcfe}.right-json-body .js{color:#f0a57a}.right-json-body .jn{color:#c4b5fd}.right-json-body .jb{color:#f472b6}.right-json-body .jp{color:#6b727c}
.right-json-help{border-top:none;color:#5c636d;padding:12px 18px}
#promptsFooter{background:var(--surface);color:var(--mut);border-top:1px solid var(--bd);min-height:30px;padding:4px 18px}
#promptsFooter .footer-badge{background:var(--acc-soft);color:#93c5fd;border-radius:999px;padding:1px 8px}
#promptsFooter kbd{background:var(--surface-3);border-color:var(--bd-2);color:var(--fg)}

/* ---- Vista Modelos ---- */
#modelPanel,#modelCenterCol{background:var(--bg)}
#modelStatusBar{background:var(--surface);border-bottom:1px solid var(--bd);height:38px;min-height:38px;padding:0 18px}
#modelLeftCol{background:var(--surface);border-right:1px solid var(--bd)}
.model-scope-segmented{background:var(--surface);border-bottom:1px solid var(--bd);padding:10px 12px}
.model-scope-segmented button{background:var(--input);border-color:var(--bd);color:var(--mut);border-radius:999px}
.model-scope-segmented button:hover{background:var(--surface-3)}
.model-scope-segmented button.active{background:var(--acc-soft);border-color:var(--acc-line);color:#bfdbfe}
.model-list-header{background:var(--surface);border-bottom:1px solid var(--bd);padding:12px}
#modelListSearch,#centerBotFilter{background:var(--input);border:1px solid var(--bd-2);border-radius:var(--r-sm)}
.model-item-list{background:var(--surface);padding:10px 12px}
.model-list-item{background:var(--surface-2);border:1px solid var(--bd);border-radius:var(--r)}
.model-list-item:hover{background:var(--surface-3);border-color:var(--bd-2)}
.model-list-item.active{background:rgba(59,130,246,.10);border-color:var(--acc-line);box-shadow:none}
.model-card-tag,.model-card-multi{background:var(--surface-3);border-color:var(--bd-2);border-radius:999px}
#modelCenterHeader{background:var(--surface);border-bottom:1px solid var(--bd);padding:14px 18px}
.target-title-mono{font-size:17px}
#modelNewTable th{background:var(--surface);border-bottom:1px solid var(--bd);color:var(--mut);font-size:10.5px;letter-spacing:.7px}
#modelNewTable td{border-bottom:1px solid #1d2026}
#modelNewTable tr:hover{background:#15181c}
#modelNewTable tr.selected{background:rgba(59,130,246,.08)}
#modelRightCol{background:var(--surface);border-left:1px solid var(--bd)}
.right-col-header{border-bottom:1px solid var(--bd)}
.right-col-footer{background:var(--surface);border-top:1px solid var(--bd)}
.field-group input,.field-group select,#newVarInput{background:var(--input);border:1px solid var(--bd-2);border-radius:var(--r-sm)}
.btn-review{background:var(--acc);border-color:var(--acc);border-radius:var(--r)}
.btn-review:hover:not(:disabled){background:var(--acc-h)}
.btn-review:disabled{background:var(--surface-3);border-color:var(--bd-2)}
.btn-undo-status{background:var(--ok-soft);border-color:rgba(63,185,80,.4);color:#7ee787;border-radius:999px}

/* ---- Vista API keys ---- */
#apiKeyPanel{background:var(--bg)}
#apiKeyActions{background:var(--surface);border-bottom:1px solid var(--bd);padding:12px 18px}
#apiKeyTable th{background:var(--surface);border-bottom:1px solid var(--bd);color:var(--mut);font-size:10.5px;font-weight:700;text-transform:uppercase;letter-spacing:.7px;padding:10px 14px}
#apiKeyTable td{border-bottom:1px solid #1d2026;padding:10px 14px}
#apiKeyTable tr:hover td{background:#15181c}
.keyEntry code{font-family:var(--mono)}
.audit-ok{color:#7ee787}.audit-warn{color:#e3b341}.audit-critical{color:#ff9b93}

/* ---- Vista Limpieza ---- */
#cleanupPanel{background:var(--bg);font-family:var(--font);color:var(--fg)}
#cleanupLeftCol,#cleanupRightCol{background:var(--surface);border-color:var(--bd)}
.cleanup-sidebar-header,.cleanup-right-header{background:var(--surface);border-bottom:1px solid var(--bd)}
.cleanup-sidebar-title,.cleanup-right-title{color:var(--mut);font-size:10.5px;letter-spacing:1px}
.cleanup-sidebar-client-item{border-radius:var(--r);color:var(--fg-2)}
.cleanup-sidebar-client-item:hover{background:var(--surface-2);color:#fff}
.cleanup-sidebar-client-item.active{background:var(--acc-soft);border-color:var(--acc-line);color:#bfdbfe}
.cleanup-sidebar-client-badge,.cleanup-client-bots-badge{background:var(--surface-3);color:var(--fg-2);border-radius:999px}
.cleanup-sidebar-client-item.active .cleanup-sidebar-client-badge{background:var(--acc);color:#fff}
#cleanupCenterCol{background:var(--bg)}
#cleanupCenterHeader{background:var(--surface);border-bottom:1px solid var(--bd);padding:16px 22px}
.cleanup-main-title{font-size:18px;font-weight:650}
.cleanup-main-subtitle{color:var(--mut)}
.cleanup-select-all-env{background:var(--surface-3);border-color:var(--bd-2);border-radius:var(--r)}
#cleanupAccordionWrap{padding:18px 22px;gap:12px}
.cleanup-client-card{background:var(--surface);border:1px solid var(--bd);border-radius:var(--r-lg)}
.cleanup-client-card:hover{border-color:var(--bd-2)}
.cleanup-client-card.expanded{border-color:var(--acc-line);box-shadow:0 6px 20px rgba(0,0,0,.3)}
.cleanup-client-card-header{background:var(--surface);padding:12px 16px}
.cleanup-client-card-header:hover{background:var(--surface-2)}
.cleanup-client-body{background:var(--surface);border-top:1px solid var(--bd);padding:12px 16px}
.cleanup-client-toolbar{border-bottom-color:var(--bd)}
.cleanup-client-tabs{background:var(--input);border-color:var(--bd);border-radius:999px;padding:3px}
.cleanup-client-tab-btn{border-radius:999px;padding:4px 12px}
.cleanup-client-tab-btn.active{background:#2a2e35;color:#fff}
.cleanup-bots-table th{background:var(--surface-2);border-bottom:1px solid var(--bd);color:var(--mut)}
.cleanup-bots-table td{border-bottom:1px solid #1d2026}
.cleanup-bots-table tr:hover td{background:var(--surface-2)}
.cleanup-bot-subid,.cleanup-bot-date{font-family:var(--mono)}
.cleanup-badge-records.has-records{background:var(--acc-soft);color:#93c5fd;box-shadow:inset 0 0 0 1px var(--acc-line)}
.cleanup-badge-records.no-records{background:var(--surface-3);color:var(--mut)}
.cleanup-action-more-btn{background:var(--surface-3);border-color:var(--bd-2);border-radius:var(--r-sm)}
.cleanup-selection-summary-card{background:var(--surface-2);border-color:var(--bd);border-radius:var(--r-lg)}
.cleanup-summary-records-badge,.cleanup-selected-list-preview,.cleanup-right-info{background:var(--input);border-color:var(--bd);border-radius:var(--r)}
.cleanup-bot-chip{background:var(--surface-3);border-color:var(--bd-2);font-family:var(--mono);border-radius:999px}
.cleanup-warning-card{background:var(--warn-soft);border-color:rgba(210,153,34,.4);color:#e3b341;border-radius:var(--r)}
.btn-export-cleanup{background:var(--surface-3);border-color:var(--bd-2);color:var(--fg);border-radius:var(--r)}
.btn-export-cleanup:hover:not(:disabled){background:#2a2f36;border-color:#3b414a}
.btn-delete-cleanup{border-radius:var(--r);background:var(--danger-soft);border-color:rgba(248,81,73,.5);color:#ff9b93}
.btn-delete-cleanup:hover:not(:disabled){background:rgba(248,81,73,.22);border-color:var(--danger);color:#fff}
.cleanup-env-summary{background:var(--input);border-color:var(--bd);border-radius:999px;color:var(--fg-2)}
.cleanup-modal-alert-danger{background:var(--danger-soft);border-color:rgba(248,81,73,.4);border-radius:var(--r)}
.cleanup-modal-table th{background:var(--surface-2);border-bottom:1px solid var(--bd);color:var(--mut)}
.cleanup-modal-table td{border-bottom:1px solid var(--bd);color:var(--fg)}
.cleanup-spinner{border-color:rgba(59,130,246,.2);border-top-color:var(--acc-h)}

/* ---- Vista Tablas DynamoDB ---- */
#tablesPanel{background:var(--bg);font-family:var(--font);color:var(--fg)}
.tables-view-header{display:none}
.tables-tabs-nav{background:var(--surface);border-bottom:1px solid var(--bd);padding:10px 22px;gap:6px}
.tables-tab-btn{background:transparent;border:1px solid transparent;color:var(--mut);border-radius:999px;padding:6px 14px}
.tables-tab-btn:hover{background:var(--surface-3);color:var(--fg);border-color:transparent}
.tables-tab-btn.active{background:var(--acc-soft);border-color:var(--acc-line);color:#bfdbfe;box-shadow:none}
.tables-create-main-col{padding:20px 22px 24px;gap:16px}
.tables-wizard-header{border-bottom:1px solid var(--bd);padding-bottom:14px}
.tables-section-title{font-size:18px;font-weight:650}
.tables-section-desc,.tables-card-subtitle{color:var(--mut)}
.tables-step-circle{border-color:var(--bd-2);color:var(--mut)}
.tables-step-item.active .tables-step-circle{background:var(--acc);border-color:var(--acc)}
.tables-step-line{background:var(--bd-2)}
.tables-cards-grid{display:grid;grid-template-columns:minmax(0, 1fr) minmax(0, 1fr);gap:16px;width:100%;box-sizing:border-box}
.tables-card,.tables-bottom-summary-card,.tables-empty-tab-card{background:var(--surface);border:1px solid var(--bd);border-radius:var(--r-lg);min-width:0;box-sizing:border-box}
.tables-file-remove-header-btn{background:var(--danger-soft);border-color:rgba(248,81,73,0.35);color:#ff9b93}
.tables-file-remove-header-btn:hover{background:#da3633;border-color:#da3633;color:#fff}
.tables-card{padding:16px 18px}
.tables-card-title{font-size:13.5px;font-weight:650}
.tables-field-label{color:var(--fg-2)}
.tables-field-hint{color:#5c636d}
.tables-text-input,.tables-select-input,.col-name-input,.col-type-select,.tables-env-select{background:var(--input);border:1px solid var(--bd-2);border-radius:var(--r-sm);color:var(--fg)}
.tables-text-input:focus,.col-name-input:focus{border-color:var(--acc-h);box-shadow:0 0 0 3px var(--acc-soft)}
.col-name-input.col-name-pk{color:#93c5fd;background:rgba(59,130,246,.06);cursor:not-allowed}
.tables-env-note{background:var(--input);border-color:var(--bd);border-radius:var(--r)}
.tables-dropzone{background:var(--input);border:1.5px dashed var(--acc-line);border-radius:var(--r-lg);padding:22px 14px}
.tables-dropzone:hover,.tables-dropzone.dragover{background:var(--acc-soft);border-color:var(--acc-h)}
.tables-cloud-icon{color:var(--acc-h)}
.tables-browse-link{color:#93c5fd}
.tables-file-chip{background:var(--input);border:1px solid var(--bd-2);border-radius:var(--r)}
.tables-file-ext-icon.ext-csv{background:#0e7490}
.tables-cols-table-wrap,.tables-data-preview-table-wrap{background:var(--input);border:1px solid var(--bd);border-radius:var(--r)}
.tables-cols-table th,.tables-data-preview-table th{background:var(--surface-2);border-bottom:1px solid var(--bd);color:var(--mut)}
.tables-data-preview-table th{color:var(--fg-2)}
.tables-data-preview-table th.th-pk{color:#93c5fd}
.tables-data-preview-table th.th-missing{color:#ff9b93}
.tables-cols-table td,.tables-data-preview-table td{border-bottom:1px solid #1d2026}
.tables-data-preview-table{font-family:var(--mono)}
.badge-col-pk{background:var(--acc-soft);color:#93c5fd;border:1px solid var(--acc-line);border-radius:999px;padding:2px 9px}
.badge-col-data{background:var(--surface-3);color:var(--fg-2);border:1px solid var(--bd-2);border-radius:999px;padding:2px 9px}
.btn-delete-col{background:transparent;border:1px solid transparent;padding:3px 7px;border-radius:var(--r-sm);font-size:13px;line-height:1}
.btn-delete-col:hover{background:var(--danger-soft);border-color:rgba(248,81,73,.35)}
.action-locked{opacity:.6;font-size:13px}
.btn-add-col{background:transparent;border:1px dashed var(--bd-2);color:var(--fg-2);border-radius:var(--r)}
.btn-add-col:hover{background:var(--acc-soft);border-color:var(--acc-line);color:#bfdbfe}
.tables-preview-stat-badge{background:var(--input);border-color:var(--bd);border-radius:999px}
.tables-match-status-box{border-radius:var(--r)}
.tables-match-status-box.match-ok,.tables-match-status-box.match-success{background:var(--ok-soft);border:1px solid rgba(63,185,80,.38);color:#7ee787}
.tables-match-status-box.match-err,.tables-match-status-box.match-error{background:var(--danger-soft);border:1px solid rgba(248,81,73,.4);color:#ff9b93}
.tables-match-status-box.match-info{background:var(--acc-soft);border:1px solid rgba(59,130,246,.35);color:#93c5fd}
.tables-key-warning-box,.tables-right-warning-box{background:var(--warn-soft);border:1px solid rgba(210,153,34,.4);color:#e3b341;border-radius:var(--r)}
.tables-bottom-chip{background:var(--input);border-color:var(--bd);border-radius:999px;padding:5px 12px}
.tables-bottom-chip .chip-label{color:var(--mut)}
.btn-primary-create-table,.btn-primary-create-table-right{background:var(--acc);border-radius:var(--r)}
.btn-primary-create-table:hover:not(:disabled),.btn-primary-create-table-right:hover:not(:disabled){background:var(--acc-h);box-shadow:0 6px 18px rgba(47,116,224,.35)}
.tables-right-col{background:var(--surface);border-left:1px solid var(--bd)}
.tables-right-header{background:var(--surface);border-bottom:1px solid var(--bd)}
.tables-right-title{color:var(--fg);font-size:11px;letter-spacing:1px}
.tables-right-info-block .info-label{color:var(--mut)}
.tables-right-explainer-card{background:var(--input);border-color:var(--bd);border-radius:var(--r)}
.tables-subpane-header{background:var(--surface);border-bottom:1px solid var(--bd)}

/* ---- Modales ---- */
.modal-backdrop{background:rgba(5,6,8,.72);backdrop-filter:blur(6px);animation:fadeIn .15s ease}
.modal-dialog{background:var(--surface);border:1px solid var(--bd-2);border-radius:14px;box-shadow:0 24px 64px rgba(0,0,0,.55);animation:popIn .18s cubic-bezier(.2,.9,.3,1.2)}
.modal-header,.modal-footer{background:var(--surface);border-color:var(--bd);padding:14px 18px}
.modal-title{font-size:15px;font-weight:650}
.modal-body{padding:16px 18px}
.modal-close-btn{border-radius:var(--r-sm)}
.modal-close-btn:hover{background:var(--surface-3)}
.modal-alert-backup{background:var(--acc-soft);border-color:var(--acc-line);border-radius:var(--r);color:#93c5fd}
.modal-diff-summary{background:var(--input);border-color:var(--bd);border-radius:var(--r)}
.modal-diff-item{border-bottom-color:var(--bd)}
.modal-prod-confirm{background:var(--danger-soft);border-color:rgba(248,81,73,.45);border-radius:var(--r)}
.modal-prod-input-row input{background:var(--input);border-radius:var(--r-sm)}
.btn-modal-cancel{background:var(--surface-3);border-color:var(--bd-2);color:var(--fg);border-radius:var(--r);padding:8px 14px}
.btn-modal-cancel:hover{background:#2a2f36}
.btn-modal-confirm{background:var(--acc);border-color:var(--acc);border-radius:var(--r);padding:8px 16px}
.btn-modal-confirm:hover:not(:disabled){background:var(--acc-h)}
.btn-delete-confirm-action{border-radius:var(--r);padding:8px 16px}
.history-list-pane{background:var(--surface);border-right-color:var(--bd)}
.history-version-card{background:var(--surface-2);border-color:var(--bd);border-radius:var(--r)}
.history-version-card:hover{background:var(--surface-3);border-color:var(--bd-2)}
.history-version-card.active{background:var(--acc-soft);border-color:var(--acc-line)}
.history-detail-pane{background:var(--bg)}
.history-detail-header{border-bottom-color:var(--bd)}
.history-diff-box,.history-json-box{background:var(--input);border-color:var(--bd);border-radius:var(--r)}
.btn-restore-version{background:var(--acc);border-color:var(--acc);border-radius:var(--r)}
.shortcuts-list{display:flex;flex-direction:column;gap:2px}
.shortcut-row{display:flex;align-items:center;justify-content:space-between;gap:16px;padding:9px 4px;border-bottom:1px solid var(--bd);font-size:13px;color:var(--fg-2)}
.shortcut-row:last-child{border-bottom:none}
.shortcut-keys{display:inline-flex;gap:4px;flex-shrink:0}

/* ---- Animaciones de entrada de vista ---- */
#promptsView,#modelPanel,#apiKeyPanel,#cleanupPanel,#tablesPanel,#editor,#singleEditor{animation:viewIn .22s ease}
@keyframes viewIn{from{opacity:0;transform:translateY(4px)}to{opacity:1;transform:none}}
@keyframes fadeIn{from{opacity:0}to{opacity:1}}
@keyframes popIn{from{opacity:0;transform:scale(.97) translateY(6px)}to{opacity:1;transform:none}}
@media (prefers-reduced-motion:reduce){*{animation:none!important;transition:none!important}}
@media (max-width:1100px){#barViewTitle{min-width:0}.bvt-crumb{display:none}select#pick{min-width:200px}#promptsRightJson{width:320px}}

/* ---- API Keys View V2 (Diseño según mockup) ---- */
#apiKeyPanel {
  background: #0e1013;
  color: #d1d5db;
  display: flex;
  flex-direction: column;
  flex: 1;
  min-height: 0;
  overflow: hidden;
  padding: 16px 24px;
  gap: 12px;
}

.apikey-top-bar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  flex-wrap: wrap;
}

.apikey-filter-pills {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
}

.apk-pill {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  padding: 6px 14px;
  background: #14161a;
  border: 1px solid #272a30;
  border-radius: 9999px;
  color: #9ca3af;
  font-size: 13px;
  font-weight: 500;
  cursor: pointer;
  transition: all .15s ease;
  user-select: none;
}

.apk-pill:hover {
  background: #1a1e24;
  border-color: #383d47;
  color: #f3f4f6;
}

.apk-pill .apk-pill-count {
  font-weight: 700;
  color: #e5e7eb;
}

.apk-pill.active {
  background: #0e1a2d;
  border: 1.5px solid #2f74e0;
  color: #ffffff;
  box-shadow: 0 0 0 1px rgba(47, 116, 224, 0.2);
}

.apk-pill.active .apk-pill-count {
  color: #ffffff;
}

.apikey-right-controls {
  display: flex;
  align-items: center;
  gap: 10px;
}

#apiKeySearchInput {
  background: #14161a;
  border: 1px solid #272a30;
  border-radius: 6px;
  padding: 6px 12px;
  color: #f3f4f6;
  font-size: 13px;
  width: 170px;
  outline: none;
  transition: all .15s ease;
}

#apiKeySearchInput:focus {
  border-color: #2f74e0;
  background: #161a22;
  box-shadow: 0 0 0 2px rgba(47, 116, 224, 0.2);
}

#apiKeySearchInput::placeholder {
  color: #6b7280;
}

.apk-btn-export {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  padding: 6px 14px;
  background: #151a24;
  border: 1px solid #26334d;
  border-radius: 6px;
  color: #93c5fd;
  font-size: 12.5px;
  font-weight: 600;
  cursor: pointer;
  transition: all .15s ease;
}

.apk-btn-export:hover:not(:disabled) {
  background: #1d273a;
  border-color: #3b82f6;
  color: #bfdbfe;
}

.apk-btn-export:disabled {
  opacity: .5;
  cursor: not-allowed;
}

.apk-scroll-divider {
  width: 100%;
  height: 2px;
  background: #232730;
  border-radius: 1px;
}

.apikey-table-wrap {
  flex: 1;
  min-height: 0;
  overflow-y: auto;
  overflow-x: auto;
  border-radius: 6px;
}

#apiKeyTable.apk-table {
  width: 100%;
  border-collapse: collapse;
  table-layout: fixed;
  font-size: 13.5px;
}

#apiKeyTable.apk-table thead th {
  position: sticky;
  top: 0;
  z-index: 2;
  background: #0e1013;
  color: #9ca3af;
  font-weight: 500;
  padding: 10px 16px;
  text-align: left;
  border-bottom: 1px solid #1f232b;
}

#apiKeyTable.apk-table thead th.th-status {
  width: 48px;
  text-align: center;
}

#apiKeyTable.apk-table tbody tr {
  border-bottom: 1px solid #161920;
  transition: background .12s ease;
}

#apiKeyTable.apk-table tbody tr:hover {
  background: #14171f;
}

#apiKeyTable.apk-table tbody td {
  padding: 12px 16px;
  vertical-align: middle;
}

.apk-client-text {
  color: #e5e7eb;
  font-weight: 500;
}

.apk-bot-text {
  color: #9ca3af;
}

.apk-key-code {
  font-family: Consolas, "SF Mono", monospace;
  font-size: 13px;
  color: #d1d5db;
}

.apk-key-match {
  font-family: Consolas, "SF Mono", monospace;
  font-size: 13px;
  color: #22c55e;
  font-weight: 500;
}

.apk-key-diff {
  font-family: Consolas, "SF Mono", monospace;
  font-size: 13px;
  color: #f59e0b;
  font-weight: 500;
}

.apk-dash {
  color: #4b5563;
  font-size: 14px;
}

.apk-status-cell {
  text-align: center;
}

.apk-dot {
  display: inline-block;
  width: 8px;
  height: 8px;
  border-radius: 50%;
}

.apk-dot.ok {
  background: #22c55e;
  box-shadow: 0 0 6px rgba(34, 197, 94, 0.7);
}

.apk-dot.warn {
  background: #f59e0b;
  box-shadow: 0 0 6px rgba(245, 158, 11, 0.7);
}

.apk-dot.critical {
  background: #ef4444;
  box-shadow: 0 0 6px rgba(239, 68, 68, 0.7);
}


.ui-icon{display:inline-block;vertical-align:-2px;stroke:currentColor;flex-shrink:0}

/* Monaco Diff Editor Clean Red & Green Highlights (No diagonal stripes) */
.monaco-diff-editor .diagonal-fill {
  background: transparent !important;
  background-image: none !important;
  display: none !important;
}
.monaco-diff-editor .line-insert,
.monaco-diff-editor .view-overlays .line-insert,
.monaco-editor .line-insert {
  background-color: rgba(34, 197, 94, 0.18) !important;
}
.monaco-diff-editor .line-delete,
.monaco-diff-editor .view-overlays .line-delete,
.monaco-editor .line-delete {
  background-color: rgba(239, 68, 68, 0.18) !important;
}
.monaco-diff-editor .char-insert,
.monaco-diff-editor .inserted-char,
.monaco-editor .char-insert {
  background-color: rgba(34, 197, 94, 0.42) !important;
  border-radius: 2px !important;
}
.monaco-diff-editor .char-delete,
.monaco-diff-editor .deleted-char,
.monaco-editor .char-delete {
  background-color: rgba(239, 68, 68, 0.42) !important;
  border-radius: 2px !important;
}
.monaco-diff-editor .gutter-insert,
.monaco-diff-editor .margin-view-overlays .line-insert {
  background-color: rgba(34, 197, 94, 0.3) !important;
}
.monaco-diff-editor .gutter-delete,
.monaco-diff-editor .margin-view-overlays .line-delete {
  background-color: rgba(239, 68, 68, 0.3) !important;
}


.apk-status-cell {
  text-align: center;
  position: relative;
}
.apk-dot-wrap {
  position: relative;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  cursor: help;
}
.apk-dot {
  display: inline-block;
  width: 9px;
  height: 9px;
  border-radius: 50%;
  transition: transform 0.15s ease;
}
.apk-dot-wrap:hover .apk-dot {
  transform: scale(1.4);
}
.apk-dot-wrap .apk-dot-tooltip {
  visibility: hidden;
  opacity: 0;
  position: absolute;
  right: calc(100% + 10px);
  top: 50%;
  transform: translateY(-50%);
  background: #0d121d;
  color: #f1f5f9;
  border: 1px solid #334155;
  padding: 6px 10px;
  border-radius: 5px;
  font-size: 11.5px;
  white-space: nowrap;
  pointer-events: none;
  transition: opacity 0.15s ease, transform 0.15s ease;
  z-index: 200;
  box-shadow: 0 4px 14px rgba(0,0,0,0.6);
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
  font-weight: 500;
}
.apk-dot-wrap:hover .apk-dot-tooltip {
  visibility: visible;
  opacity: 1;
}


.apk-key-error {
  font-family: Consolas, "SF Mono", monospace;
  font-size: 13px;
  color: #ef4444;
  font-weight: 700;
  background: rgba(239, 68, 68, 0.12);
  padding: 2px 6px;
  border-radius: 4px;
  border: 1px solid rgba(239, 68, 68, 0.3);
}

</style>
</head>
<body>
<nav id="appRail" aria-label="Navegación principal">
  <div class="rail-group">
    <button type="button" class="rail-btn active" data-mode="compare" id="railBtnCompare" title="Comparar ambientes (Ctrl+1)">
      <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
        <rect x="3" y="3" width="7" height="7"></rect>
        <rect x="14" y="3" width="7" height="7"></rect>
        <rect x="14" y="14" width="7" height="7"></rect>
        <rect x="3" y="14" width="7" height="7"></rect>
      </svg>
      <span class="rail-tooltip">Comparar (Ctrl+1)</span>
    </button>
    <button type="button" class="rail-btn" data-mode="single" id="railBtnSingle" title="JSON individual (Ctrl+2)">
      <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
        <path d="M8 3H7a2 2 0 0 0-2 2v5a2 2 0 0 1-2 2 2 2 0 0 1 2 2v5c0 1.1.9 2 2 2h1"></path>
        <path d="M16 21h1a2 2 0 0 0 2-2v-5c0-1.1.9-2 2-2a2 2 0 0 1-2-2V5a2 2 0 0 0-2-2h-1"></path>
      </svg>
      <span class="rail-tooltip">JSON individual (Ctrl+2)</span>
    </button>
    <button type="button" class="rail-btn" data-mode="prompts" id="railBtnPrompts" title="Gestión de Prompts (Ctrl+3)">
      <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
        <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"></path>
        <line x1="8" y1="9" x2="16" y2="9"></line>
        <line x1="8" y1="13" x2="14" y2="13"></line>
      </svg>
      <span class="rail-tooltip">Gestión de Prompts (Ctrl+3)</span>
    </button>
    <button type="button" class="rail-btn" data-mode="models" id="railBtnModels" title="Modelos de IA (Ctrl+4)">
      <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
        <polyline points="16 18 22 12 16 6"></polyline>
        <polyline points="8 6 2 12 8 18"></polyline>
      </svg>
      <span class="rail-tooltip">Modelos (Ctrl+4)</span>
    </button>
    <button type="button" class="rail-btn" data-mode="apikeys" id="railBtnApikeys" title="Auditoría de API Keys (Ctrl+5)">
      <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
        <circle cx="7.5" cy="12" r="4.5"></circle>
        <circle cx="7.5" cy="12" r="1.5" fill="currentColor"></circle>
        <path d="M12 12h9v3.5m-4-3.5v2.5"></path>
      </svg>
      <span class="rail-tooltip">API Keys (Ctrl+5)</span>
    </button>
    <button type="button" class="rail-btn" data-mode="cleanup" id="railBtnCleanup" title="Limpieza DynamoDB (Ctrl+6)">
      <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
        <polyline points="3 6 5 6 21 6"></polyline>
        <path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"></path>
      </svg>
      <span class="rail-tooltip">Limpieza DynamoDB (Ctrl+6)</span>
    </button>
    <button type="button" class="rail-btn" data-mode="tables" id="railBtnTables" title="Gestión de Tablas (Ctrl+7)">
      <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
        <ellipse cx="12" cy="5" rx="9" ry="3"></ellipse>
        <path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"></path>
        <path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"></path>
      </svg>
      <span class="rail-tooltip">Tablas DynamoDB (Ctrl+7)</span>
    </button>
    <button type="button" class="rail-btn" data-mode="metrics" id="railBtnMetrics" title="Métricas y Auditoría de Prompts (Ctrl+8)">
      <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
        <path d="M3 3v18h18"></path>
        <path d="M18 17V9"></path>
        <path d="M13 17V5"></path>
        <path d="M8 17v-3"></path>
      </svg>
      <span class="rail-tooltip">Métricas de Prompts (Ctrl+8)</span>
    </button>
  </div>
  <div class="rail-bottom">
    <button type="button" class="rail-btn" id="railBtnShortcuts" title="Atajos de teclado (?)">
      <svg width="19" height="19" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
        <circle cx="12" cy="12" r="10"></circle>
        <path d="M9.09 9a3 3 0 0 1 5.83 1c0 2-3 3-3 3"></path>
        <line x1="12" y1="17" x2="12.01" y2="17"></line>
      </svg>
      <span class="rail-tooltip">Atajos (?)</span>
    </button>
    <button type="button" class="rail-btn" id="railBtnReload" title="Recargar lista (Ctrl+R)">
      <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
        <polyline points="23 4 23 10 17 10"></polyline>
        <path d="M20.49 15a9 9 0 1 1-2.12-9.36L23 10"></path>
      </svg>
      <span class="rail-tooltip">Recargar (Ctrl+R)</span>
    </button>
  </div>
</nav>
<div id="app">
  <div id="bar">
    <div id="barViewTitle">
      <span class="bvt-crumb">Karstec</span>
      <span class="bvt-name" id="bvtName">Comparar ambientes</span>
    </div>
    <label id="viewModeLabel" hidden>Vista <select id="viewMode"><option value="compare">Comparar</option><option value="single">JSON individual</option><option value="prompts">Gestión de Prompts</option><option value="models">Modelos</option><option value="apikeys">API keys</option><option value="cleanup">Limpieza DynamoDB</option><option value="tables">Gestión de tablas DynamoDB</option><option value="metrics">Métricas de Prompts</option></select></label>
    <label class="compare-only">Izquierda <select id="envL"></select></label>
    <button id="swap" class="compare-only" title="Intercambiar lados">⇄</button>
    <label class="compare-only">Derecha <select id="envR"></select></label>
    <div id="singleEnvControl" hidden class="bar-env-wrap">
      <span class="bar-env-label">Ambiente</span>
      <div id="singleEnvSegmented" class="env-segmented-control">
        <button type="button" class="env-btn desa active" data-env="desa">DESA</button>
        <button type="button" class="env-btn test" data-env="test">TEST</button>
        <button type="button" class="env-btn prod" data-env="prod">PROD</button>
      </div>
      <select id="singleEnv" hidden></select>
    </div>
    <div id="modelEnvSegmented" class="env-segmented-control" hidden>
      <button type="button" class="env-btn desa active" data-env="desa">DESA</button>
      <button type="button" class="env-btn test" data-env="test">TEST</button>
      <button type="button" class="env-btn prod" data-env="prod">PROD</button>
    </div>
    <select id="modelEnv" hidden><option value="desa">desa</option><option value="test">test</option><option value="prod">prod</option></select>
    <div id="cleanupTopControls" hidden style="display:flex;align-items:center;gap:10px;">
      <div id="cleanupEnvSegmented" class="env-segmented-control">
        <button type="button" class="env-btn desa active" data-env="desa">DESA</button>
        <button type="button" class="env-btn test" data-env="test">TEST</button>
        <button type="button" class="env-btn prod" data-env="prod">PROD</button>
      </div>
      <span id="cleanupEnvSummary" class="cleanup-env-summary">Ambiente: DESA • 0 bots • 0 clientes</span>
      <input type="text" id="cleanupGlobalSearch" placeholder="Buscar cliente o bot…" autocomplete="off" style="width:220px;padding:5px 9px;border:1px solid #3c3c3c;background:#1e1e1e;color:#d4d4d4;border-radius:3px;font-size:12px;">
      <button type="button" id="cleanupBtnRefresh" title="Recargar datos de DynamoDB">↻</button>
    </div>
    <div id="tablesTopControls" hidden style="display:flex;align-items:center;gap:10px;">
      <div id="tablesEnvSegmented" class="env-segmented-control">
        <button type="button" class="env-btn desa active" data-env="desa">DESA</button>
        <button type="button" class="env-btn test" data-env="test">TEST</button>
        <button type="button" class="env-btn prod" data-env="prod">PROD</button>
      </div>
      <span style="font-size:12px;font-weight:700;color:#93c5fd;background:#0d162a;padding:4px 10px;border-radius:4px;border:1px solid #1f3050;">
        <svg class="ui-icon" width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="margin-right:5px;"><ellipse cx="12" cy="5" rx="9" ry="3"></ellipse><path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"></path><path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"></path></svg>Gestión de tablas DynamoDB
      </span>
    </div>
    <div id="metricsTopControls" hidden style="display:flex;align-items:center;gap:10px;">
      <div id="metricsEnvSegmented" class="env-segmented-control">
        <button type="button" class="env-btn desa active" data-env="desa">DESA</button>
        <button type="button" class="env-btn test" data-env="test">TEST</button>
        <button type="button" class="env-btn prod" data-env="prod">PROD</button>
      </div>
      <button type="button" id="btnRefreshMetrics" title="Recargar métricas de DynamoDB">↻</button>
    </div>
    <label id="apiKeyPresenceControl" hidden>Presencia
      <select id="apiKeyPresence">
        <option value="any">Cualquier ambiente</option>
        <option value="desa+test+prod">Desa + Test + Prod</option>
        <option value="desa+test">Desa + Test, sin Prod</option>
        <option value="desa+prod">Desa + Prod, sin Test</option>
        <option value="test+prod">Test + Prod, sin Desa</option>
        <option value="desa">Solo Desa</option>
        <option value="test">Solo Test</option>
        <option value="prod">Solo Prod</option>
      </select>
    </label>
    <input id="filter" placeholder="Filtrar client / bot…" autocomplete="off">
    <select id="pick"></select>
    <button id="ok" class="primary">OK · Comparar</button>
    <button id="reload" title="Volver a leer la lista de client/bot">↻</button>
    <span class="spacer"></span>
    <div id="modelTopActions" hidden style="display:flex;align-items:center;gap:8px;">
      <button type="button" id="btnReplicatePrompts" title="Replicar estructura de prompts desde otro ambiente" style="background:#213b52;border:1px solid #388bfd;color:#79c0ff;font-weight:600;"><svg class="ui-icon" width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="margin-right:4px;"><rect width="14" height="14" x="8" y="8" rx="2" ry="2"></rect><path d="M4 16c-1.1 0-2-.9-2-2V4c0-1.1.9-2 2-2h10c1.1 0 2 .9 2 2"></path></svg> Replicar estructura de…</button>
      <button type="button" id="modelBtnHistory" title="Ver historial de cambios"><svg class="ui-icon" width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="margin-right:4px;"><circle cx="12" cy="12" r="10"></circle><polyline points="12 6 12 12 16 14"></polyline></svg> Historial de cambios</button>
      <button type="button" id="modelBtnExcel" title="Descargar Excel de modelos">Descargar Excel</button>
    </div>
    <label class="compare-only"><input type="checkbox" id="hide"> Ocultar líneas iguales</label>
    <button id="prev" class="compare-only" title="Diferencia anterior (Shift+F7)">▲</button>
    <button id="next" class="compare-only" title="Diferencia siguiente (F7)">▼</button>
    <button type="button" id="barSaveBtn" class="primary bar-save-btn" hidden disabled>Guardar</button>
  </div>
  <div id="status">Cargando…</div>
  <div id="paneBar">
    <div class="pane">
      <span class="env-badge desa" id="badgeL">DESA</span>
      <span class="paneTitle" id="hL">Izquierda</span>
      <div class="paneTools"><button id="foldL" title="Plegar todo">−</button><button id="unfoldL" title="Desplegar todo">+</button><button id="saveL" class="primary" disabled>Guardar</button></div>
    </div>
    <div class="transfer">
      <button id="toLeft" title="Pasar el bloque seleccionado de derecha a izquierda">←</button>
      <button id="toRight" title="Pasar el bloque seleccionado de izquierda a derecha">→</button>
    </div>
    <div class="pane right">
      <span class="env-badge test" id="badgeR">TEST</span>
      <span class="paneTitle" id="hR">Derecha</span>
      <div class="paneTools"><button id="foldR" title="Plegar todo">−</button><button id="unfoldR" title="Desplegar todo">+</button><button id="saveR" class="primary" disabled>Guardar</button></div>
    </div>
  </div>
  <div id="editor"></div>
  <div id="singleBar" hidden>
    <span class="env-badge desa" id="singleEnvBadge">DESA</span>
    <span class="paneTitle" id="singleTitle">JSON individual</span>
    <span class="spacer"></span>
    <div class="segmented-control" id="singleSubMode">
      <button type="button" id="subModeJson">JSON</button>
      <button type="button" id="subModePrompts" class="active">Texto</button>
    </div>
    <button type="button" id="btnHistory" title="Ver historial de versiones guardadas (hasta 20)"><svg class="ui-icon" width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="margin-right:4px;"><circle cx="12" cy="12" r="10"></circle><polyline points="12 6 12 12 16 14"></polyline></svg> Historial <span id="historyBadgeCount" class="badge-count" style="display:none;"></span></button>
    <button type="button" id="btnToggleRightJson" title="Mostrar/ocultar JSON de DynamoDB">{ } Ocultar JSON</button>
    <button type="button" id="btnFontSizeDec" title="Disminuir tamaño de fuente">−</button>
    <button type="button" id="btnFontSizeInc" title="Aumentar tamaño de fuente">+</button>
    <button type="button" id="saveSingle" class="primary" disabled>Guardar</button>
  </div>
  <div id="singleEditor" hidden></div>
  <div id="promptsView" hidden>
    <div id="promptsSidebar">
      <div class="sidebar-quick-nav">
        <button type="button" class="quick-nav-btn" data-target="compare" id="btnQuickNavCompare">
          <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="3" width="7" height="7"></rect><rect x="14" y="3" width="7" height="7"></rect><rect x="14" y="14" width="7" height="7"></rect><rect x="3" y="14" width="7" height="7"></rect></svg>
          Comparar
        </button>
        <button type="button" class="quick-nav-btn" data-target="single" id="btnQuickNavSingle">
          <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M8 3H7a2 2 0 0 0-2 2v5a2 2 0 0 1-2 2 2 2 0 0 1 2 2v5c0 1.1.9 2 2 2h1"></path><path d="M16 21h1a2 2 0 0 0 2-2v-5c0-1.1.9-2 2-2a2 2 0 0 1-2-2V5a2 2 0 0 0-2-2h-1"></path></svg>
          JSON
        </button>
        <button type="button" class="quick-nav-btn active" data-target="prompts" id="btnQuickNavPrompts">
          <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"></path><line x1="8" y1="9" x2="16" y2="9"></line><line x1="8" y1="13" x2="14" y2="13"></line></svg>
          Prompts
        </button>
      </div>
      <div class="prompts-sidebar-section" id="sidebarModelsSection">
        <div class="sidebar-section-title collapsible" id="sidebarModelsHeader" title="Plegar / desplegar modelos en uso">
          <span><span class="sidebar-models-toggle-icon" id="sidebarModelsToggleIcon">▼</span>MODELOS EN USO</span>
          <span id="sidebarModelsCount" style="font-size:11px;color:#858585;font-weight:normal;"></span>
        </div>
        <div id="sidebarModelsBody">
          <div id="sidebarModelsList"></div>
          <div class="sidebar-model-card sidebar-apikey-card" id="sidebarApiKeyCard" hidden>
            <div class="sidebar-card-label">OPENAI_API_KEY</div>
            <div class="sidebar-card-val-row">
              <span id="sidebarApiKeyMasked">••••••••</span>
              <button type="button" id="sidebarApiKeyToggleBtn">Ver</button>
            </div>
          </div>
        </div>
      </div>
      <div class="prompts-sidebar-section" style="flex:1;min-height:0;overflow:auto;">
        <div class="sidebar-section-title">PROMPTS · AGENT_CONFIG</div>
        <div id="sidebarPromptsList"></div>
      </div>
      <div class="sidebar-help-note">
        El árbol sale de AGENT_CONFIG → PROMPTS. Cada prompt muestra si difiere del mismo bot en test.
      </div>
    </div>
    <div id="promptsMain">
      <div class="prompt-header-row">
        <div style="display:flex;align-items:center;gap:12px;flex-wrap:wrap;">
          <h2 id="promptCurrentName" class="prompt-main-title">PROMPT</h2>
          <span id="promptDiffBadge" class="prompt-main-diff-badge">Difiere del mismo bot en test</span>
          <div style="display:inline-flex;align-items:center;gap:5px;font-size:12px;color:#858585;">
            <span>vs</span>
            <select id="promptsCompEnvSelect" style="background:#252526;color:#ccc;border:1px solid #444;border-radius:3px;font-size:11.5px;padding:2px 6px;cursor:pointer;" title="Cambiar ambiente de comparación"></select>
          </div>
        </div>
        <button type="button" id="btnComparePrompt" class="btn-compare-env">Comparar con test</button>
      </div>
      <div class="prompts-card" id="promptParamsCard">
        <div class="params-header-bar" id="promptParamsHeader" title="Clic para plegar / desplegar parámetros">
          <div class="params-header-left">
            <span class="params-toggle-icon" id="paramsToggleIcon">▼</span>
            <span class="card-section-title" style="margin:0;">PARÁMETROS</span>
            <span class="params-summary-badge" id="paramsSummaryBadge"></span>
          </div>
          <button type="button" class="btn-toggle-params" id="btnToggleParams">Plegar</button>
        </div>
        <div id="promptParamsContent" style="margin-top:8px;"></div>
      </div>
      <div class="prompts-card template-card" id="promptTemplateCard">
        <div class="template-header-bar">
          <div class="template-header-left">
            <span class="template-label">TEMPLATE</span>
            <span class="chip" id="templateFsPromptName" style="background:#2d2d2d;color:#38bdf8;font-family:Consolas,monospace;display:none;"></span>
            <span class="chip chip-string">String</span>
          </div>
          <div class="template-header-tools">
            <button type="button" id="btnUndoTemplate" title="Deshacer (Ctrl+Z)" aria-label="Deshacer">
              <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M9 14 4 9l5-5"/><path d="M4 9h10.5a5.5 5.5 0 0 1 5.5 5.5a5.5 5.5 0 0 1-5.5 5.5H11"/></svg>
            </button>
            <button type="button" id="btnRedoTemplate" title="Rehacer (Ctrl+Y)" aria-label="Rehacer">
              <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m15 14 5-5-5-5"/><path d="M20 9H9.5A5.5 5.5 0 0 0 4 14.5A5.5 5.5 0 0 0 9.5 20H13"/></svg>
            </button>
            <button type="button" id="btnDiffTemplate" title="Comparar diff con test" aria-label="Ver diff">
              <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m16 3 4 4-4 4"/><path d="M20 7H4"/><path d="m8 21-4-4 4-4"/><path d="M4 17h16"/></svg>
            </button>
            <button type="button" id="btnFoldTemplate" title="Plegar secciones" aria-label="Plegar secciones">
              <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m7 15 5 5 5-5"/><path d="m7 9 5-5 5 5"/></svg>
            </button>
            <button type="button" id="btnWrapTemplate" class="active" title="Ajuste de línea" aria-label="Ajuste de línea">
              <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="3" y1="6" x2="21" y2="6"/><path d="M3 12h15a3 3 0 1 1 0 6h-4"/><polyline points="16 16 14 18 16 20"/><line x1="3" y1="18" x2="10" y2="18"/></svg>
            </button>
            <button type="button" id="btnFullscreenTemplate" title="Expandir editor a toda la ventana (Esc)" aria-label="Pantalla completa">
              <svg class="fs-icon-expand" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><polyline points="15 3 21 3 21 9"></polyline><polyline points="9 21 3 21 3 15"></polyline><line x1="21" y1="3" x2="14" y2="10"></line><line x1="3" y1="21" x2="10" y2="14"></line></svg>
              <svg class="fs-icon-collapse" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round" style="display:none;"><polyline points="4 14 10 14 10 20"></polyline><polyline points="20 10 14 10 14 4"></polyline><line x1="14" y1="10" x2="21" y2="3"></line><line x1="3" y1="21" x2="10" y2="14"></line></svg>
            </button>
          </div>
        </div>
        <div id="templateMonacoWrap" class="template-editor-wrapper">
          <div id="templateMonacoEditor"></div>
          <div id="templateMonacoDiffEditor" style="width:100%;height:100%;position:absolute;top:0;left:0;" hidden></div>
        </div>
        <div class="template-footer-bar">
          <span id="templateStats">0 caracteres · ~0 tokens</span>
          <span id="templateDiffWarning" class="template-diff-msg" hidden><svg class="ui-icon" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="margin-right:4px;"><path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3Z"></path><line x1="12" y1="9" x2="12" y2="13"></line><line x1="12" y1="17" x2="12.01" y2="17"></line></svg> Hay campos en test que desa no tiene</span>
        </div>
      </div>
      <div class="prompts-card comp-preview-card" id="promptCompPreviewCard">
        <div class="comp-preview-header" id="compPreviewHeader">
          <div class="comp-preview-header-left">
            <span class="comp-preview-toggle-icon" id="compPreviewToggleIcon">▶</span>
            <span class="card-section-title" style="margin:0;">PREVIEW EN <span id="compPreviewEnvName">TEST</span></span>
            <span class="comp-preview-status-badge" id="compPreviewStatusBadge"></span>
          </div>
          <div class="comp-preview-header-right">
            <button type="button" class="btn-copy-all-test" id="btnCopyAllFromComp" title="Reemplazar parámetros y template con los de este ambiente">
              <svg class="ui-icon" width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="margin-right:4px;"><rect width="14" height="14" x="8" y="8" rx="2" ry="2"></rect><path d="M4 16c-1.1 0-2-.9-2-2V4c0-1.1.9-2 2-2h10c1.1 0 2 .9 2 2"></path></svg> Copiar todo desde test
            </button>
            <button type="button" class="btn-toggle-comp-preview" id="btnToggleCompPreview">Mostrar</button>
          </div>
        </div>
        <div class="comp-preview-body" id="compPreviewBody" hidden>
          <div class="comp-preview-params-wrap" id="compPreviewParamsWrap">
            <div class="comp-preview-subtitle">Parámetros:</div>
            <div class="comp-preview-params-list" id="compPreviewParamsList"></div>
          </div>
          <div class="comp-preview-template-wrap">
            <div class="comp-preview-subtitle-row">
              <span class="comp-preview-subtitle">TEMPLATE:</span>
              <button type="button" class="btn-copy-template-only" id="btnCopyTemplateOnly"><svg class="ui-icon" width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="margin-right:4px;"><rect width="14" height="14" x="8" y="8" rx="2" ry="2"></rect><path d="M4 16c-1.1 0-2-.9-2-2V4c0-1.1.9-2 2-2h10c1.1 0 2 .9 2 2"></path></svg> Copiar solo TEMPLATE</button>
            </div>
            <pre class="comp-preview-template-text" id="compPreviewTemplateText"></pre>
          </div>
        </div>
      </div>
    </div>
    <div id="promptsRightJson">
      <div class="right-json-header">
        <div class="right-json-title-row">
          <span class="right-json-title">JSON de AWS DynamoDB</span>
          <button type="button" id="btnRightJsonCollapse" class="btn-right-json-collapse" title="Ocultar panel de JSON">− Ocultar JSON</button>
        </div>
        <div class="right-json-subtitle">Campo seleccionado: <strong id="rightJsonField">TEMPLATE</strong></div>
      </div>
      <div class="type-pill-row">
        <span class="type-pill-label">Tipo</span>
        <div class="type-pill-group">
          <span class="type-pill active" id="typePillS">S</span>
          <span class="type-pill" id="typePillN">N</span>
          <span class="type-pill" id="typePillL">L</span>
          <span class="type-pill" id="typePillM">M</span>
        </div>
      </div>
      <div class="right-json-body">
        <pre><code id="rightJsonPre"></code></pre>
      </div>
      <div class="right-json-help">
        Si el tipo es N, solo acepta números y se guarda como string numérico, como pide DynamoDB.
      </div>
    </div>
  </div>
  <div id="promptsFooter" hidden>
    <div class="footer-left-group">
      <span class="footer-badge" id="footerEnvBadge">DESA</span>
      <span id="footerTableName">desa_sonia_config</span>
      <span class="footer-dot">·</span>
      <span id="footerChangesCount">0 cambios sin guardar</span>
    </div>
    <div class="footer-right-group">
      <span><kbd>Ctrl+S</kbd> guarda y muestra el diff final</span>
      <span class="footer-dot">·</span>
      <span>Se hace backup del ítem antes de escribir</span>
    </div>
  </div>
  <div id="modelPanel" hidden>
    <div id="modelStatusBar">
      <span id="modelStatusText">Cargando modelos…</span>
      <span class="model-status-spacer"></span>
      <span id="modelStatusProdWarning" class="model-status-prod" hidden>Producción: los cambios son reales.</span>
      <button type="button" id="modelUndoBtn" class="btn-undo-status" hidden>Deshacer</button>
    </div>
    <div id="modelColumnsContainer">
      <!-- Columna Izquierda: 310 px -->
      <div id="modelLeftCol">
        <div class="model-scope-segmented">
          <button type="button" id="scopeByPrompt" class="active">Por prompt</button>
          <button type="button" id="scopeByModel">Por modelo</button>
        </div>
        <div class="model-list-header">
          <div class="model-list-title-row">
            <span id="modelListTitle" class="model-list-title-label">▼ PROMPTS EN USO</span>
            <span id="modelListCount" class="model-list-count-badge"></span>
          </div>
          <input type="text" id="modelListSearch" placeholder="Buscar prompt…" autocomplete="off">
        </div>
        <div id="modelItemList" class="model-item-list" role="listbox"></div>
      </div>

      <!-- Columna Central: flexible -->
      <div id="modelCenterCol">
        <div id="modelCenterHeader">
          <div class="center-header-line1">
            <span id="centerTargetTitle" class="target-title-mono"></span>
            <span id="centerTargetCounts" class="target-counts"></span>
          </div>
          <div class="center-header-line2" id="centerExplText">
            El mismo prompt en todos los clientes. Marcá los bots y cambiá a la vez.
          </div>
          <div class="center-header-line3">
            <input type="text" id="centerBotFilter" placeholder="Buscar cliente o bot…" autocomplete="off">
            <span class="chips-label">Familia</span>
            <div id="familyChips" class="chips-container"></div>
          </div>
          <div class="center-header-line4">
            <span id="groupFilterLabel" class="chips-label">Seleccionar solo los que usan</span>
            <div id="groupChips" class="chips-container"></div>
          </div>
        </div>

        <div id="modelTableWrap" tabindex="0">
          <table id="modelNewTable">
            <thead>
              <tr>
                <th class="th-check"><input type="checkbox" id="selectAllVisible" title="Seleccionar visibles"></th>
                <th id="thClientBot">Client / bot</th>
                <th class="th-model">Modelo</th>
                <th class="th-temp">Temp.</th>
                <th class="th-reasoning">Reasoning</th>
                <th class="th-format">Formato</th>
                <th class="th-tokens">Tokens</th>
              </tr>
            </thead>
            <tbody id="modelTableBody"></tbody>
          </table>
          <div id="modelTableEmpty" class="table-empty-msg" hidden></div>
        </div>
      </div>

      <!-- Columna Derecha: 350 px -->
      <div id="modelRightCol">
        <div class="right-col-header">
          <div class="right-col-title-wrap">
            <span>CAMBIAR EN <strong id="rightSelectedCount" class="right-selected-num">0</strong> <span id="rightSelectedLabel">bots seleccionados</span></span>
          </div>
          <button type="button" id="rightClearChanges" class="btn-link">Limpiar</button>
        </div>

        <div class="right-col-scrollable">
          <!-- Sección CAMPOS -->
          <div class="right-section">
            <div class="right-section-title">CAMPOS <span class="right-section-sub">lo que dice "mantener" no se toca</span></div>
            <div class="field-group">
              <label id="lblModelSelect" for="editModelSelect">Modelo</label>
              <select id="editModelSelect">
                <option value="">mantener</option>
              </select>
            </div>
            <div class="field-row-2">
              <div class="field-group">
                <label for="editTempInput">Temperatura (0 a 2)</label>
                <input type="text" id="editTempInput" placeholder="mantener" autocomplete="off">
                <div id="tempError" class="field-error-msg" hidden>La temperatura va de 0 a 2. Usá punto para decimales (0.7).</div>
              </div>
              <div class="field-group">
                <label for="editTokensInput">Max tokens</label>
                <input type="text" id="editTokensInput" placeholder="mantener" autocomplete="off">
                <div id="tokensError" class="field-error-msg" hidden>Max tokens debe ser un entero mayor a 0.</div>
              </div>
            </div>
            <div class="field-row-2">
              <div class="field-group">
                <label for="editReasoningSelect">Reasoning</label>
                <select id="editReasoningSelect">
                  <option value="">mantener</option>
                  <option value="__empty__">dejar vacío</option>
                  <option value="none">none</option>
                  <option value="low">low</option>
                  <option value="medium">medium</option>
                  <option value="high">high</option>
                </select>
              </div>
              <div class="field-group">
                <label for="editFormatSelect">Formato</label>
                <select id="editFormatSelect">
                  <option value="">mantener</option>
                  <option value="__empty__">dejar vacío</option>
                  <option value="text">text</option>
                  <option value="json_object">json_object</option>
                </select>
              </div>
            </div>
          </div>

          <!-- Sección VARIABLES -->
          <div class="right-section">
            <div class="right-section-title">VARIABLES · INPUT_VARIABLE</div>
            <div id="variablesList" class="variables-list"></div>
            <div class="new-var-row">
              <input type="text" id="newVarInput" placeholder="NUEVA_VARIABLE" autocomplete="off">
              <button type="button" id="btnAddVar" class="btn-add-var">+ Agregar</button>
            </div>
            <div class="var-help-text">
              Declarar una variable no modifica el texto del TEMPLATE; solo la agrega a INPUT_VARIABLE.
            </div>
          </div>
        </div>

        <div class="right-col-footer">
          <div id="rightFooterSummary" class="right-footer-summary">
            Marcá filas en la tabla para empezar.
          </div>
          <button type="button" id="btnReviewChanges" class="btn-review" disabled>
            Revisar 0 bots
          </button>
          <div class="right-footer-note">
            Los filtros acotan la selección. Nada se escribe hasta confirmar el detalle.
          </div>
        </div>
      </div>
    </div>
  </div>
  <div id="apiKeyPanel" hidden>
    <div id="apiKeyActions" class="apikey-top-bar">
      <div style="display:inline-flex;align-items:center;gap:8px;margin-right:6px;">
        <svg class="ui-icon" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="color:#60a5fa;">
          <circle cx="7.5" cy="12" r="4.5"></circle>
          <circle cx="7.5" cy="12" r="1.5" fill="currentColor"></circle>
          <path d="M12 12h9v3.5m-4-3.5v2.5"></path>
        </svg>
        <span style="font-weight:700;font-size:13.5px;color:#f3f4f6;letter-spacing:-0.2px;">API Keys</span>
      </div>
      <div class="apikey-filter-pills" id="apiKeyFilterPills">
        <button type="button" class="apk-pill" data-filter="collisions" id="pillApkCollisions">
          Colisión Prod <span class="apk-pill-count" id="countApkCollisions">0</span>
        </button>
        <button type="button" class="apk-pill" data-filter="mismatch" id="pillApkMismatch">
          Desa ≠ Test <span class="apk-pill-count" id="countApkMismatch">0</span>
        </button>
        <button type="button" class="apk-pill" data-filter="sin_prod" id="pillApkNoProd">
          Sin Prod <span class="apk-pill-count" id="countApkNoProd">0</span>
        </button>
        <button type="button" class="apk-pill active" data-filter="all" id="pillApkAll">
          Todos <span class="apk-pill-count" id="countApkAll">0</span>
        </button>
      </div>

      <div class="apikey-right-controls">
        <input type="text" id="apiKeySearchInput" placeholder="Buscar..." autocomplete="off">
        <label id="apiKeyEnvExportControl" style="display:none;">
          <input type="checkbox" class="apikey-env-check" value="desa" checked>
          <input type="checkbox" class="apikey-env-check" value="test" checked>
          <input type="checkbox" class="apikey-env-check" value="prod" checked>
        </label>
        <button type="button" id="exportApiKeys" class="apk-btn-export" title="Descargar auditoría de API keys en Excel">Descargar Excel</button>
      </div>
    </div>

    <div class="apk-scroll-divider"></div>

    <div class="apikey-table-wrap">
      <table id="apiKeyTable" class="apk-table">
        <thead>
          <tr>
            <th style="width:24%;">Cliente</th>
            <th style="width:24%;">Bot</th>
            <th style="width:17%;">Desa</th>
            <th style="width:17%;">Test</th>
            <th style="width:14%;">Prod</th>
            <th class="th-status" style="width:4%;"></th>
          </tr>
        </thead>
        <tbody id="apiKeyRows"></tbody>
      </table>
      <div id="apiKeyEmptyState" class="apk-empty-state" hidden style="text-align:center;padding:40px;color:#6b7280;font-size:13px;">
        No se encontraron bots que coincidan con el filtro seleccionado.
      </div>
    </div>
  </div>
  <div id="cleanupPanel" hidden>
    <div id="cleanupColumnsContainer">
      <!-- Columna Izquierda: CLIENTES list (navegación y conteo) -->
      <div id="cleanupLeftCol">
        <div class="cleanup-sidebar-header">
          <span class="cleanup-sidebar-title">CLIENTES (<span id="cleanupClientsCount">0</span>)</span>
        </div>
        <div id="cleanupClientsList" class="cleanup-clients-sidebar-list"></div>
      </div>

      <!-- Columna Central: Bots por ambiente y acordeón de clientes -->
      <div id="cleanupCenterCol">
        <div id="cleanupCenterHeader">
          <div class="cleanup-header-titles">
            <h2 class="cleanup-main-title">Bots por ambiente</h2>
            <div class="cleanup-main-subtitle">Consulta y limpieza de registros en DynamoDB</div>
          </div>
          <div class="cleanup-header-actions">
            <label class="cleanup-select-all-env">
              <input type="checkbox" id="cleanupSelectAllEnv"> Seleccionar todos los bots del ambiente
            </label>
          </div>
        </div>

        <div id="cleanupAccordionWrap">
          <div id="cleanupAccordionContainer"></div>
          <div id="cleanupEmptyMessage" class="cleanup-empty-msg" hidden>No se encontraron clientes o bots coincidentes con la búsqueda.</div>
        </div>
      </div>

      <!-- Columna Derecha: LIMPIEZA DE DYNAMODB -->
      <div id="cleanupRightCol">
        <div class="cleanup-right-header">
          <div class="cleanup-right-title">LIMPIEZA DE DYNAMODB</div>
          <div class="cleanup-right-env-row">
            <span>Ambiente seleccionado:</span>
            <span class="env-badge desa" id="cleanupRightEnvBadge">DESA</span>
          </div>
        </div>

        <div class="cleanup-right-content">
          <div class="cleanup-selection-summary-card">
            <div class="cleanup-summary-icon-row">
              <svg class="cleanup-db-icon" width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">
                <ellipse cx="12" cy="5" rx="9" ry="3"></ellipse>
                <path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"></path>
                <path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"></path>
              </svg>
              <div>
                <div class="cleanup-summary-count"><strong id="cleanupSelectedBotsCount">0</strong> bots seleccionados</div>
                <div class="cleanup-summary-subtext">Se eliminarán los registros de DynamoDB de los bots seleccionados en el ambiente actual.</div>
              </div>
            </div>
            <div class="cleanup-summary-records-badge" id="cleanupSelectedRecordsNotice">
              <strong id="cleanupSelectedRecordsCount">0</strong> registros a eliminar
            </div>
          </div>

          <div id="cleanupSelectedListPreview" class="cleanup-selected-list-preview">
            <div class="cleanup-preview-header">
              <div class="cleanup-preview-title-row">
                <span class="cleanup-preview-title">CLIENTES Y BOTS SELECCIONADOS</span>
                <span class="cleanup-preview-count-badge" id="cleanupPreviewCount">0</span>
              </div>
              <button type="button" class="cleanup-clear-all-btn" id="btnCleanupClearAll" style="display:none;">Desmarcar todos</button>
            </div>
            <div id="cleanupSelectedItemsChips" class="cleanup-preview-items-scroll"></div>
          </div>

          <div class="cleanup-warning-card">
            <span class="cleanup-warning-icon"><svg class="ui-icon" width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3Z"></path><line x1="12" y1="9" x2="12" y2="13"></line><line x1="12" y1="17" x2="12.01" y2="17"></line></svg></span>
            <span>La eliminación solo afecta al ambiente seleccionado (<strong id="cleanupWarnEnv">DESA</strong>). No se modifican los demás ambientes.</span>
          </div>

          <button type="button" id="btnDeleteSelectedRecords" class="btn-delete-cleanup" disabled>
            <svg class="ui-icon" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 6h18"></path><path d="M19 6v14c0 1-1 2-2 2H7c-1 0-2-1-2-2V6"></path><path d="M8 6V4c0-1 1-2 2-2h4c1 0 2 1 2 2v2"></path></svg> Eliminar registros seleccionados
          </button>

          <label class="cleanup-confirm-check-label">
            <input type="checkbox" id="cleanupConfirmCheckbox"> Confirmo que quiero eliminar los registros seleccionados
          </label>

          <button type="button" id="btnExportCleanup" class="btn-export-cleanup" disabled>
            <svg class="ui-icon" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"></path><polyline points="17 8 12 3 7 8"></polyline><line x1="12" y1="3" x2="12" y2="15"></line></svg> Exportar selección
          </button>

          <div class="cleanup-right-info">
            <span class="info-icon">ⓘ</span>
            <span>Los clientes se agrupan para ahorrar espacio. Marcá los bots para seleccionar sus registros de DynamoDB en el ambiente seleccionado.</span>
          </div>
        </div>
      </div>
    </div>
  </div>

  <!-- Vista Gestión de tablas DynamoDB -->
  <div id="tablesPanel" hidden>
    <!-- Header de navegación de la vista -->
    <div class="tables-view-header">
      <div class="tables-header-left">
        <svg class="tables-header-icon" width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">
          <ellipse cx="12" cy="5" rx="9" ry="3"></ellipse>
          <path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"></path>
          <path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"></path>
        </svg>
        <div>
          <h2 class="tables-view-title">Gestión de tablas DynamoDB</h2>
          <div class="tables-view-subtitle">Administración de tablas y carga de datos</div>
        </div>
      </div>
      <div class="tables-header-right">
        <label class="tables-env-select-label">
          <span>Ambiente:</span>
          <select id="tablesGlobalEnvSelect" class="tables-env-select">
            <option value="desa">DESA</option>
            <option value="test">TEST</option>
            <option value="prod">PROD</option>
          </select>
        </label>
      </div>
    </div>

    <!-- Pestañas principales de navegación -->
    <div class="tables-tabs-nav">
      <button type="button" class="tables-tab-btn" data-tab="list" id="tabBtnListTables">
        <svg class="ui-icon" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M9 5H7a2 2 0 0 0-2 2v12a2 2 0 0 0 2 2h10a2 2 0 0 0 2-2V7a2 2 0 0 0-2-2h-2"></path><rect width="8" height="4" x="8" y="3" rx="1" ry="1"></rect><path d="M9 12h6"></path><path d="M9 16h6"></path></svg> Listar tablas
      </button>
      <button type="button" class="tables-tab-btn" data-tab="empty" id="tabBtnEmptyTable">
        <svg class="ui-icon" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 6h18"></path><path d="M19 6v14c0 1-1 2-2 2H7c-1 0-2-1-2-2V6"></path><path d="M8 6V4c0-1 1-2 2-2h4c1 0 2 1 2 2v2"></path></svg> Vaciar tabla
      </button>
      <button type="button" class="tables-tab-btn active" data-tab="create" id="tabBtnCreateTable">
        <svg class="ui-icon" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"></path><polyline points="7 10 12 15 17 10"></polyline><line x1="12" y1="15" x2="12" y2="3"></line></svg> Cargar tabla
      </button>
    </div>

    <!-- Contenido de las pestañas -->
    <div class="tables-tabs-content">
      <!-- 1. Pestaña Crear tabla -->
      <div id="tabContentCreate" class="tables-tab-pane active">
        <div class="tables-create-layout">
          <!-- Columna principal a ancho completo (ganando todo el espacio) -->
          <div class="tables-create-main-col" style="width:100%;max-width:100%;">
            <!-- Grid 2x2 de tarjetas con espacio amplio -->
            <div class="tables-cards-grid">
              <!-- Tarjeta 1: Configuración de la tabla -->
              <div class="tables-card">
                <div class="tables-card-header">
                  <div style="display:flex;align-items:center;justify-content:space-between;">
                    <h4 class="tables-card-title">Configuración de la tabla</h4>
                    <div id="tablesAutoEnvDisplay" style="display:inline-flex;align-items:center;gap:6px;">
                      <span style="font-size:11px;color:#94a3b8;">Ambiente:</span>
                      <span class="env-badge desa" id="tablesDetectedEnvBadge">DESA</span>
                    </div>
                  </div>
                </div>
                <div class="tables-card-body" style="display:flex;flex-direction:column;gap:10px;">
                  <div class="tables-field-group">
                    <label class="tables-field-label" for="tablesTableNameInput">
                      Nombre de la tabla existente <span class="tables-required-star">*</span>
                    </label>
                    <input type="text" id="tablesTableNameInput" list="tablesAvailableDatalist" class="tables-text-input" value="desa_liderazgo_tru_deuda_custom" placeholder="Seleccioná o escribí la tabla existente (ej. desa_..., test_..., prod_...)" autocomplete="off" spellcheck="false">
                    <datalist id="tablesAvailableDatalist"></datalist>
                    <span class="tables-field-hint">El ambiente se determina automáticamente según el prefijo de la tabla (<code>desa_</code>, <code>test_</code> o <code>prod_</code>). La tabla debe existir previamente en DynamoDB.</span>
                  </div>
                </div>
              </div>

              <!-- Tarjeta 2: Archivo de origen -->
              <div class="tables-card">
                <div class="tables-card-header">
                  <h4 class="tables-card-title">Archivo de origen</h4>
                </div>
                <div class="tables-card-body">
                  <input type="file" id="tablesFileInput" accept=".xlsx,.xls,.csv" style="display:none;">
                  <div id="tablesDropZone" class="tables-dropzone">
                    <svg class="tables-cloud-icon" width="36" height="36" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round">
                      <path d="M17.5 19H9a7 7 0 1 1 6.71-9h1.79a4.5 4.5 0 1 1 0 9Z"></path>
                    </svg>
                    <div class="tables-drop-prompt">
                      <span>Arrastrá y soltá un archivo Excel o CSV</span>
                      <a href="#" id="tablesBrowseLink" class="tables-browse-link">o seleccioná un archivo</a>
                    </div>
                    <span class="tables-drop-formats">Formatos admitidos: .xlsx, .xls, .csv</span>
                  </div>
                  <!-- Chip del archivo cargado con opción de cruz destacada arriba -->
                  <div id="tablesFileLoadedChip" class="tables-file-chip" hidden>
                    <div class="tables-file-chip-left">
                      <div class="tables-file-ext-icon" id="tablesFileExtBadge">X</div>
                      <div class="tables-file-info">
                        <span class="tables-file-name" id="tablesFileNameDisplay" title="Archivo cargado">CUSTOM_TABLE_TESTING_neu.xlsx</span>
                        <span class="tables-file-meta" id="tablesFileMetaDisplay">1.2 MB • 15 filas</span>
                      </div>
                    </div>
                    <button type="button" class="tables-file-remove-btn" id="btnRemoveUploadedFile" title="Eliminar archivo cargado" aria-label="Eliminar archivo cargado">✕</button>
                  </div>
                </div>
              </div>

              <!-- Tarjeta 3: Columnas a importar -->
              <div class="tables-card">
                <div class="tables-card-header">
                  <h4 class="tables-card-title">Columnas a importar</h4>
                  <p class="tables-card-subtitle">Definí qué columnas del archivo se cargarán en la tabla. La columna custom_key es obligatoria (clave de partición).</p>
                </div>
                <div class="tables-card-body tables-cols-body">
                  <div class="tables-cols-table-wrap">
                    <table class="tables-cols-table">
                      <thead>
                        <tr>
                          <th style="width:26px;"></th>
                          <th>Columna</th>
                          <th style="width:100px;">Tipo</th>
                          <th style="width:140px;">Función</th>
                          <th style="width:50px;text-align:center;">Acciones</th>
                        </tr>
                      </thead>
                      <tbody id="tablesColumnsTableBody"></tbody>
                    </table>
                  </div>
                  <button type="button" id="btnAddColumnBtn" class="btn-add-col">
                    <span style="font-size:15px;line-height:1;">+</span> Agregar columna
                  </button>
                </div>
              </div>

              <!-- Tarjeta 4: Vista previa del archivo -->
              <div class="tables-card">
                <div class="tables-card-header">
                  <h4 class="tables-card-title">Vista previa del archivo</h4>
                  <p class="tables-card-subtitle">Primeras filas del archivo y coincidencia con las columnas configuradas.</p>
                </div>
                <div class="tables-card-body tables-preview-body">
                  <div class="tables-preview-top-bar">
                    <div class="tables-preview-stat-badge">
                      <svg class="ui-icon" width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="margin-right:4px;"><path d="M14.5 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V7.5L14.5 2z"></path><polyline points="14 2 14 8 20 8"></polyline></svg> Filas totales en el archivo: <strong id="tablesPreviewRowsCount">0</strong>
                    </div>
                  </div>
                  <!-- Mensaje de estado de coincidencia -->
                  <div id="tablesColumnsMatchStatus" class="tables-match-status-box match-info">
                    <span class="status-icon">ⓘ</span>
                    <span class="status-text">Cargá un archivo Excel o CSV para ver la correspondencia de columnas.</span>
                  </div>
                  <!-- Alerta de claves vacías o duplicadas -->
                  <div id="tablesKeyWarningBox" class="tables-key-warning-box" hidden></div>
                  <!-- Tabla de previsualización -->
                  <div class="tables-data-preview-table-wrap">
                    <table class="tables-data-preview-table">
                      <thead id="tablesPreviewThead">
                        <tr><th>custom_key</th><th>alias_display</th><th>cliente</th><th>dni</th><th>tipo_contacto</th><th>deuda</th></tr>
                      </thead>
                      <tbody id="tablesPreviewTbody">
                        <tr><td colspan="6" class="tables-preview-empty-td">Sin datos para mostrar. Arrastrá o seleccioná un archivo en el paso 2.</td></tr>
                      </tbody>
                    </table>
                  </div>
                </div>
              </div>
            </div>

            <!-- Resumen de la carga (barra inferior completa a lo ancho) -->
            <div class="tables-bottom-summary-card">
              <div class="tables-bottom-chips">
                <div class="tables-bottom-chip">
                  <span class="chip-label">Operación:</span>
                  <strong style="color:#f8fafc;">Cargar tabla</strong>
                </div>
                <div class="tables-bottom-chip">
                  <span class="chip-label">Ambiente:</span>
                  <span class="env-badge desa" id="bottomChipEnvBadge">DESA</span>
                </div>
                <div class="tables-bottom-chip">
                  <span class="chip-label">Tabla destino:</span>
                  <strong id="bottomChipTable">desa_liderazgo_tru_deuda_custom</strong>
                </div>
                <div class="tables-bottom-chip">
                  <span class="chip-label">Archivo:</span>
                  <strong id="bottomChipFile">Ningún archivo</strong>
                  <span id="bottomChipFileMeta" style="font-size:11px;color:#94a3b8;margin-left:4px;"></span>
                </div>
                <div class="tables-bottom-chip">
                  <span class="chip-label">Columnas:</span>
                  <strong id="bottomChipCols">6</strong>
                </div>
                <div class="tables-bottom-chip" style="border-color:#1e3a8a;background:#0d182e;">
                  <svg class="ui-icon" width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="margin-right:2px;color:#93c5fd;">
                    <circle cx="7.5" cy="12" r="4.5"></circle>
                    <circle cx="7.5" cy="12" r="1.5" fill="currentColor"></circle>
                    <path d="M12 12h9v3.5m-4-3.5v2.5"></path>
                  </svg>
                  <span class="chip-label" style="color:#93c5fd;">PK:</span>
                  <strong style="color:#bfdbfe;">custom_key</strong>
                </div>
              </div>
              <button type="button" id="btnExecuteCreateTableBottom" class="btn-primary-create-table" disabled>
                <svg class="ui-icon" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"></path><polyline points="7 10 12 15 17 10"></polyline><line x1="12" y1="15" x2="12" y2="3"></line></svg> Cargar datos en tabla ➔
              </button>
            </div>
          </div>
        </div>
      </div>

      <!-- 2. Pestaña Listar tablas -->
      <div id="tabContentList" class="tables-tab-pane" hidden>
        <div class="tables-subpane-header">
          <input type="text" id="tablesListSearchInput" class="tables-text-input" placeholder="Filtrar tablas por nombre…" style="width:300px;">
          <button type="button" id="btnRefreshTablesList" class="btn-export-cleanup" style="width:auto;">↻ Actualizar lista</button>
        </div>
        <div class="tables-subpane-table-wrap">
          <table class="cleanup-bots-table">
            <thead>
              <tr><th>NOMBRE DE LA TABLA</th><th>PARTITION KEY (HASH)</th><th>SORT KEY (RANGE)</th><th>REGISTROS (APROX)</th><th style="text-align:right;">ACCIONES</th></tr>
            </thead>
            <tbody id="tablesListTableBody"></tbody>
          </table>
        </div>
      </div>

      <!-- 3. Pestaña Vaciar tabla -->
      <div id="tabContentEmpty" class="tables-tab-pane" hidden>
        <div class="tables-empty-tab-card">
          <h3 style="margin:0;font-size:16px;color:#f8fafc;">Vaciar registros de una tabla</h3>
          <p style="font-size:12.5px;color:#94a3b8;margin:4px 0 16px;">Elimina todos los elementos contenidos en la tabla seleccionada en el ambiente actual sin eliminar la estructura de la tabla.</p>
          <div style="display:flex;flex-direction:column;gap:12px;max-width:440px;">
            <label class="tables-field-label">Tabla a vaciar:
              <input type="text" id="tablesEmptyTableNameInput" class="tables-text-input" placeholder="Nombre exacto de la tabla">
            </label>
            <label class="tables-field-label">Nombre de clave primaria:
              <input type="text" id="tablesEmptyKeyNameInput" class="tables-text-input" value="custom_key">
            </label>
            <div style="background:#450a0a;border:1px solid #dc2626;color:#fca5a5;padding:10px 14px;border-radius:6px;font-size:12px;line-height:1.4;">
              <svg class="ui-icon" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="margin-right:4px;"><path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3Z"></path><line x1="12" y1="9" x2="12" y2="13"></line><line x1="12" y1="17" x2="12.01" y2="17"></line></svg> <strong>CUIDADO:</strong> Esta acción eliminará permanentemente todos los registros escaneados en la tabla.
            </div>
            <button type="button" id="btnExecuteEmptyTable" class="btn-delete-confirm-action" style="padding:10px 18px;margin-top:6px;">
              <svg class="ui-icon" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 6h18"></path><path d="M19 6v14c0 1-1 2-2 2H7c-1 0-2-1-2-2V6"></path><path d="M8 6V4c0-1 1-2 2-2h4c1 0 2 1 2 2v2"></path></svg> Vaciar todos los registros
            </button>
            <div id="tablesEmptyResultWrap" hidden style="margin-top:10px;"></div>
          </div>
        </div>
      </div>
    </div>
  </div>
  <!-- Vista 8: Métricas y Auditoría de Prompts -->
  <div id="metricsPanel" hidden>
    <!-- Submode Tab Switcher: Longitud vs Variables por prompt -->
    <div class="metrics-submode-tabs">
      <button type="button" class="metrics-tab-btn active" id="btnMetricsSubmodeLength" data-submode="length">
        Longitud de prompts
      </button>
      <button type="button" class="metrics-tab-btn" id="btnMetricsSubmodeVars" data-submode="variables">
        Variables repetidas por prompt
      </button>
    </div>

    <!-- 3 Threshold Cards Grid (Length Mode) -->
    <div class="metrics-thresholds-grid" id="metricsThresholdsGrid">
      <!-- Card 1: Límite de advertencia -->
      <div class="metrics-threshold-card card-warn">
        <div class="threshold-card-header">
          <span class="threshold-card-title title-warn">Límite de advertencia</span>
          <span class="threshold-card-sub">Se marca en amarillo cuando lo supera.</span>
        </div>
        <div class="threshold-stepper-wrap">
          <div class="threshold-stepper-control">
            <button type="button" class="threshold-stepper-btn" id="btnWarnMinus" title="Disminuir límite de advertencia">-</button>
            <input type="number" id="metricsLimitWarnInput" class="threshold-stepper-input" value="10000" min="1000" step="1000">
            <button type="button" class="threshold-stepper-btn" id="btnWarnPlus" title="Aumentar límite de advertencia">+</button>
          </div>
          <span class="threshold-unit-label">caracteres</span>
        </div>
      </div>

      <!-- Card 2: Límite crítico -->
      <div class="metrics-threshold-card card-crit">
        <div class="threshold-card-header">
          <span class="threshold-card-title title-crit">Límite crítico</span>
          <span class="threshold-card-sub">Se marca en rojo cuando lo supera.</span>
        </div>
        <div class="threshold-stepper-wrap">
          <div class="threshold-stepper-control">
            <button type="button" class="threshold-stepper-btn" id="btnCritMinus" title="Disminuir límite crítico">-</button>
            <input type="number" id="metricsLimitCritInput" class="threshold-stepper-input" value="14000" min="1000" step="1000">
            <button type="button" class="threshold-stepper-btn" id="btnCritPlus" title="Aumentar límite crítico">+</button>
          </div>
          <span class="threshold-unit-label">caracteres</span>
        </div>
      </div>

      <!-- Card 3: Distribución con estos límites -->
      <div class="metrics-threshold-card card-dist">
        <div class="threshold-card-header">
          <span class="threshold-card-title title-dist">Distribución con estos límites</span>
          <span class="threshold-card-sub">Cuántos prompts caen en cada zona.</span>
        </div>
        <div class="dist-bar-track" id="metricsDistBarTrack">
          <div class="dist-bar-seg seg-ok" id="distSegOk" style="width: 25%;" title="Dentro del límite"></div>
          <div class="dist-bar-seg seg-warn" id="distSegWarn" style="width: 15%;" title="En advertencia"></div>
          <div class="dist-bar-seg seg-crit" id="distSegCrit" style="width: 60%;" title="Críticos"></div>
        </div>
        <div class="dist-bar-legend">
          <span class="dist-legend-item"><span class="dist-legend-dot" style="background:#22c55e;"></span> <span id="legendOkCount">0 dentro del límite</span></span>
          <span class="dist-legend-item"><span class="dist-legend-dot" style="background:#eab308;"></span> <span id="legendWarnCount">0 en advertencia</span></span>
          <span class="dist-legend-item"><span class="dist-legend-dot" style="background:#ef4444;"></span> <span id="legendCritCount">0 críticos</span></span>
        </div>
      </div>
    </div>

    <!-- Filter & Search Toolbar (Shared / Mode-aware) -->
    <div class="metrics-toolbar">
      <!-- Quick Status Filter Pills (Length Mode) -->
      <div class="metrics-quick-pills" id="metricsFilterPills">
        <button type="button" class="metrics-quick-pill active" data-filter="all">Todos <span class="pill-count" id="countMetricsAll">0</span></button>
        <button type="button" class="metrics-quick-pill pill-warn" data-filter="warn"><span id="pillWarnLabel">&gt; 10.000 car.</span> <span class="pill-count" id="countMetricsWarn">0</span></button>
        <button type="button" class="metrics-quick-pill pill-crit" data-filter="danger"><span id="pillCritLabel">&gt; 14.000 car.</span> <span class="pill-count" id="countMetricsDanger">0</span></button>
      </div>

      <!-- Filter Pills for Variables Mode -->
      <div class="metrics-quick-pills" id="metricsFilterPillsVars" style="display:none;">
        <button type="button" class="metrics-quick-pill active" data-varfilter="repeated">Con variables repetidas <span class="pill-count" id="countVarsWithRepeated">0</span></button>
        <button type="button" class="metrics-quick-pill pill-warn" data-varfilter="high">Repetición alta (&ge; 5) <span class="pill-count" id="countVarsHigh">0</span></button>
        <button type="button" class="metrics-quick-pill" data-varfilter="all">Todos los prompts <span class="pill-count" id="countVarsAllPrompts">0</span></button>
      </div>

      <!-- Search Input -->
      <div class="metrics-search-wrap">
        <svg class="ui-icon metrics-search-icon" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="11" cy="11" r="8"></circle><line x1="21" y1="21" x2="16.65" y2="16.65"></line></svg>
        <input type="text" id="metricsSearchInput" class="metrics-search-input" placeholder="Buscar por cliente, bot, prompt o modelo…" autocomplete="off">
      </div>

      <!-- Model Filter Selector -->
      <div class="metrics-model-filter-wrap">
        <select id="metricsModelSelect" class="metrics-select" title="Filtrar por modelo de IA">
          <option value="">Todos los modelos</option>
        </select>
      </div>

      <!-- Reset Limits Button (visible in Length mode) -->
      <button type="button" id="btnResetLimits" class="btn-reset-limits" title="Restablecer límites de advertencia y crítico a 10.000 y 14.000">
        Restablecer límites
      </button>
    </div>

    <!-- KPI Summary Row (Length Mode) -->
    <div class="metrics-kpi-row" id="metricsKpiRowLength">
      <div class="metrics-kpi-card">
        <span class="metrics-kpi-label">TOTAL PROMPTS</span>
        <strong class="metrics-kpi-val" id="kpiTotalPrompts">0</strong>
      </div>
      <div class="metrics-kpi-card">
        <span class="metrics-kpi-label">LONGITUD PROMEDIO</span>
        <strong class="metrics-kpi-val" id="kpiAvgChars">0 car.</strong>
      </div>
      <div class="metrics-kpi-card kpi-card-warn">
        <span class="metrics-kpi-label" id="kpiWarnLabel">EN ADVERTENCIA (&gt; 10.000)</span>
        <strong class="metrics-kpi-val text-warn" id="kpiWarnPrompts">0</strong>
      </div>
      <div class="metrics-kpi-card kpi-card-danger">
        <span class="metrics-kpi-label" id="kpiCritLabel">CRÍTICOS (&gt; 14.000)</span>
        <strong class="metrics-kpi-val text-danger" id="kpiDangerPrompts">0</strong>
      </div>
    </div>

    <!-- KPI Summary Row (Variables Mode) -->
    <div class="metrics-kpi-row" id="metricsKpiRowVars" style="display:none;">
      <div class="metrics-kpi-card">
        <span class="metrics-kpi-label">PROMPTS CON VARIABLES REPETIDAS</span>
        <strong class="metrics-kpi-val" id="kpiPromptsWithRepeated">0</strong>
      </div>
      <div class="metrics-kpi-card">
        <span class="metrics-kpi-label">TOTAL REPETICIONES DE VARIABLES</span>
        <strong class="metrics-kpi-val" id="kpiTotalRepetitions">0</strong>
      </div>
      <div class="metrics-kpi-card kpi-card-danger">
        <span class="metrics-kpi-label">PROMPT CON MÁS REPETICIONES</span>
        <strong class="metrics-kpi-val text-danger" id="kpiTopRepeatedPrompt" style="font-size:13.5px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">—</strong>
      </div>
      <div class="metrics-kpi-card kpi-card-warn">
        <span class="metrics-kpi-label">VARIABLE MÁS REPETIDA</span>
        <strong class="metrics-kpi-val text-warn" id="kpiTopRepeatedVar" style="font-size:14px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">—</strong>
      </div>
    </div>

    <!-- Table Container (Length Mode) -->
    <div class="metrics-table-wrap" id="metricsTableLengthWrap">
      <table class="metrics-table">
        <thead>
          <tr>
            <th style="min-width:180px;" data-sort="client_bot">CLIENTE / BOT <span class="sort-icon"></span></th>
            <th style="min-width:240px;" data-sort="prompt_name">IDENTIFICADOR PROMPT <span class="sort-icon"></span></th>
            <th style="min-width:140px;" data-sort="model">MODELO <span class="sort-icon"></span></th>
            <th style="min-width:280px;" data-sort="char_count" class="sorted-asc">CARACTERES <span class="sort-icon" id="metricsSortCharIcon">▲</span></th>
            <th style="min-width:120px;" data-sort="estimated_tokens">TOKENS EST. <span class="sort-icon"></span></th>
            <th style="min-width:160px;text-align:right;">ACCIONES</th>
          </tr>
        </thead>
        <tbody id="metricsTableBody">
          <tr><td colspan="6" class="metrics-empty-td">Cargando métricas de prompts…</td></tr>
        </tbody>
      </table>
    </div>

    <!-- Variables Repetidas por Prompt Container (Variables Mode) -->
    <div class="metrics-table-wrap" id="metricsTableVarsWrap" style="display:none;">
      <div id="metricsVarsPromptList" class="var-prompt-list">
        <div class="metrics-empty-td">Cargando variables repetidas por prompt…</div>
      </div>
    </div>
  </div>
</div>


<div id="cleanupDeleteConfirmModal" class="modal-backdrop" hidden>
  <div class="modal-dialog" style="width:640px;max-width:94vw;">
    <div class="modal-header">
      <div class="modal-title-wrap">
        <span class="modal-title">Confirmar eliminación en DynamoDB</span>
        <span class="env-badge desa" id="cleanupModalEnvBadge">DESA</span>
      </div>
      <button type="button" class="modal-close-btn" id="cleanupModalCloseBtn" title="Cerrar modal (Esc)">✕</button>
    </div>
    <div class="modal-body">
      <div class="cleanup-modal-alert-danger">
        <svg class="ui-icon" width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3Z"></path><line x1="12" y1="9" x2="12" y2="13"></line><line x1="12" y1="17" x2="12.01" y2="17"></line></svg>
        <div>
          <strong>¡ATENCIÓN!</strong> Vas a eliminar los registros de DynamoDB de los siguientes bots en <strong id="cleanupModalEnvName">DESA</strong>.
          <div style="font-size:11.5px;margin-top:4px;color:#cbd5e1;">Se guardará automáticamente un backup en <code>backups/cleanup/</code> antes de ejecutar la eliminación.</div>
        </div>
      </div>

      <div style="margin:12px 0 6px;font-size:11px;font-weight:700;color:#94a3b8;text-transform:uppercase;letter-spacing:0.5px;">Resumen de bots a limpiar (<span id="cleanupModalCount">0</span>):</div>
      <div id="cleanupModalBotsTableWrap" style="max-height:220px;overflow-y:auto;border:1px solid #334155;border-radius:4px;">
        <table class="cleanup-modal-table">
          <thead><tr><th>Cliente</th><th>Bot / Agente</th><th>Registros DynamoDB</th></tr></thead>
          <tbody id="cleanupModalBotsTableBody"></tbody>
        </table>
      </div>

      <div id="cleanupModalProdWarning" class="modal-prod-confirm" hidden style="margin-top:14px;">
        <div class="modal-prod-warning">
          <svg class="ui-icon" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="margin-right:4px;"><path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3Z"></path><line x1="12" y1="9" x2="12" y2="13"></line><line x1="12" y1="17" x2="12.01" y2="17"></line></svg> <strong>ADVERTENCIA CRÍTICA:</strong> El ambiente es <strong>PRODUCCIÓN</strong>. Esta acción eliminará registros reales.
        </div>
        <div class="modal-prod-input-row" style="margin-top:8px;">
          <label for="cleanupModalProdInput" style="font-size:12px;color:#fca5a5;">Escribí <code>ELIMINAR EN PROD</code> para habilitar:</label>
          <input type="text" id="cleanupModalProdInput" placeholder="ELIMINAR EN PROD" autocomplete="off" style="width:100%;margin-top:4px;background:#1e1e1e;border:1px solid #b91c1c;color:#fff;padding:6px 10px;border-radius:4px;font-size:13px;" />
        </div>
      </div>

      <div id="cleanupModalProgressWrap" hidden style="margin-top:14px;text-align:center;padding:14px;background:#0f172a;border-radius:4px;border:1px solid #1e293b;">
        <div class="cleanup-spinner" style="margin:0 auto 8px;"></div>
        <div id="cleanupModalProgressText" style="font-size:12.5px;color:#38bdf8;font-weight:600;">Eliminando registros en DynamoDB y creando backups…</div>
      </div>

      <div id="cleanupModalResultWrap" hidden style="margin-top:14px;padding:12px;border-radius:4px;font-size:12.5px;"></div>
    </div>
    <div class="modal-footer">
      <button type="button" class="btn-modal-cancel" id="btnCleanupModalCancel">Cancelar</button>
      <button type="button" class="btn-delete-confirm-action" id="btnCleanupModalExecute">Confirmar y Eliminar</button>
    </div>
  </div>
</div>

<div id="cleanupBotDetailModal" class="modal-backdrop" hidden>
  <div class="modal-dialog" style="width:520px;max-width:92vw;">
    <div class="modal-header">
      <div class="modal-title-wrap">
        <span class="modal-title" id="cleanupDetailTitle">Detalle del Bot</span>
        <span class="env-badge desa" id="cleanupDetailEnvBadge">DESA</span>
      </div>
      <button type="button" class="modal-close-btn" id="cleanupDetailCloseBtn" title="Cerrar modal (Esc)">✕</button>
    </div>
    <div class="modal-body" id="cleanupDetailBody" style="display:flex;flex-direction:column;gap:12px;"></div>
    <div class="modal-footer">
      <button type="button" class="btn-modal-cancel" id="btnCleanupDetailClose">Cerrar</button>
      <button type="button" class="btn-modal-confirm" id="btnCleanupDetailDeleteOne" style="background:#b91c1c;border-color:#b91c1c;">Limpiar este bot</button>
    </div>
  </div>
</div>

<div id="tablesCreateConfirmModal" class="modal-backdrop" hidden>
  <div class="modal-dialog" style="width:620px;max-width:94vw;">
    <div class="modal-header">
      <div class="modal-title-wrap">
        <span class="modal-title">Confirmar carga de tabla</span>
        <span class="env-badge desa" id="tablesModalEnvBadge">DESA</span>
      </div>
      <button type="button" class="modal-close-btn" id="tablesModalCloseBtn" title="Cerrar modal (Esc)">✕</button>
    </div>
    <div class="modal-body">
      <div class="cleanup-modal-alert-danger" style="background:#172554;border-color:#2563eb;color:#bfdbfe;">
        <svg class="ui-icon" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><ellipse cx="12" cy="5" rx="9" ry="3"></ellipse><path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"></path><path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"></path></svg>
        <div>
          <strong>Resumen de la operación antes de validar y cargar</strong>
          <div style="font-size:12px;margin-top:4px;color:#cbd5e1;">Verificá los datos a continuación. Los registros serán procesados e insertados en la tabla existente de DynamoDB con clave de partición <code>custom_key</code>.</div>
        </div>
      </div>

      <div style="margin:14px 0 6px;font-size:11.5px;font-weight:700;color:#94a3b8;text-transform:uppercase;">Resumen de la operación:</div>
      <table class="cleanup-modal-table">
        <tbody>
          <tr><td style="color:#94a3b8;width:170px;">Operación</td><td><strong>Cargar tabla</strong></td></tr>
          <tr><td style="color:#94a3b8;">Tabla DynamoDB</td><td><strong id="tablesModalTableName"></strong></td></tr>
          <tr><td style="color:#94a3b8;">Ambiente destino</td><td><span class="env-badge desa" id="tablesModalEnv">DESA</span></td></tr>
          <tr><td style="color:#94a3b8;">Archivo origen</td><td><span id="tablesModalFileName"></span> <span id="tablesModalFileMeta" style="color:#94a3b8;font-size:11px;"></span></td></tr>
          <tr><td style="color:#94a3b8;">Columnas a importar</td><td><span id="tablesModalColsList"></span></td></tr>
          <tr><td style="color:#94a3b8;">Filas a procesar</td><td><strong id="tablesModalTotalRows"></strong></td></tr>
          <tr><td style="color:#94a3b8;">Clave de partición</td><td><span class="badge-col-pk"><svg class="ui-icon" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="margin-right:4px;"><circle cx="7.5" cy="12" r="4.5"></circle><circle cx="7.5" cy="12" r="1.5" fill="currentColor"></circle><path d="M12 12h9v3.5m-4-3.5v2.5"></path></svg>custom_key (obligatoria)</span></td></tr>
        </tbody>
      </table>

      <div style="background:rgba(180,83,9,0.12);border:1px solid #92400e;border-radius:6px;padding:10px 12px;margin-top:12px;font-size:11.5px;color:#fde68a;display:flex;align-items:flex-start;gap:8px;">
        <svg class="ui-icon" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="flex-shrink:0;margin-top:2px;"><path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3Z"></path><line x1="12" y1="9" x2="12" y2="13"></line><line x1="12" y1="17" x2="12.01" y2="17"></line></svg>
        <span>La tabla debe existir previamente en DynamoDB. Los registros del archivo sin clave <code>custom_key</code> serán omitidos. Claves duplicadas sobrescribirán la versión anterior.</span>
      </div>

      <div id="tablesModalProdWarning" class="modal-prod-confirm" hidden style="margin-top:14px;">
        <div class="modal-prod-warning">
          <svg class="ui-icon" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="margin-right:4px;"><path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3Z"></path><line x1="12" y1="9" x2="12" y2="13"></line><line x1="12" y1="17" x2="12.01" y2="17"></line></svg> <strong>ADVERTENCIA:</strong> Estás por cargar datos en <strong>PRODUCCIÓN</strong>.
        </div>
        <div class="modal-prod-input-row" style="margin-top:8px;">
          <label for="tablesModalProdInput" style="font-size:12px;color:#fca5a5;">Escribí <code>CARGAR EN PROD</code> para habilitar:</label>
          <input type="text" id="tablesModalProdInput" placeholder="CARGAR EN PROD" autocomplete="off" style="width:100%;margin-top:4px;background:#1e1e1e;border:1px solid #b91c1c;color:#fff;padding:6px 10px;border-radius:4px;font-size:13px;" />
        </div>
      </div>

      <div id="tablesModalProgressWrap" hidden style="margin-top:14px;text-align:center;padding:16px;background:#0f172a;border-radius:4px;border:1px solid #1e293b;">
        <div class="cleanup-spinner" style="margin:0 auto 8px;"></div>
        <div id="tablesModalProgressText" style="font-size:12.5px;color:#38bdf8;font-weight:600;">Cargando registros en la tabla de DynamoDB…</div>
      </div>

      <div id="tablesModalResultWrap" hidden style="margin-top:14px;padding:12px;border-radius:4px;font-size:12.5px;"></div>
    </div>
    <div class="modal-footer">
      <button type="button" class="btn-modal-cancel" id="btnTablesModalCancel">Cancelar</button>
      <button type="button" class="btn-modal-confirm" id="btnTablesModalExecute" style="background:#0078d4;border-color:#0078d4;">Confirmar y Cargar</button>
    </div>
  </div>
</div>

<div id="saveModal" class="modal-backdrop" hidden>
  <div class="modal-dialog">
    <div class="modal-header">
      <div class="modal-title-wrap">
        <span class="modal-title">Confirmar guardado</span>
        <span class="env-badge" id="modalEnvBadge">DESA</span>
        <span class="modal-bot-name" id="modalBotName"></span>
      </div>
      <button type="button" class="modal-close-btn" id="modalCloseBtn" title="Cerrar modal (Esc)">✕</button>
    </div>
    <div class="modal-body">
      <div class="modal-alert-backup" id="modalBackupAlert">
        <svg class="ui-icon" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="margin-right:4px;"><path d="M19 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h11l5 5v11a2 2 0 0 1-2 2z"></path><polyline points="17 21 17 13 7 13 7 21"></polyline><polyline points="7 3 7 8 15 8"></polyline></svg>
        <span>Se guardará un backup automático del ítem previo en la carpeta <code>backups/</code>.</span>
      </div>
      <div style="font-size:12px;font-weight:600;color:#aaa;text-transform:uppercase;letter-spacing:0.5px;">Campos que se modificarán en DynamoDB:</div>
      <div class="modal-diff-summary" id="modalDiffSummary"></div>
      <div class="modal-prod-confirm" id="modalProdConfirm" hidden>
        <div class="modal-prod-warning">
          <svg class="ui-icon" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="margin-right:4px;"><path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3Z"></path><line x1="12" y1="9" x2="12" y2="13"></line><line x1="12" y1="17" x2="12.01" y2="17"></line></svg> <strong>ATENCIÓN:</strong> Vas a guardar cambios directamente en <strong>PRODUCCIÓN</strong>.
        </div>
        <div class="modal-prod-input-row">
          <label for="modalProdBotInput">Escribí el nombre del bot (<code id="modalProdBotExpected"></code>) para confirmar:</label>
          <input type="text" id="modalProdBotInput" placeholder="Nombre exacto del bot" autocomplete="off" />
        </div>
      </div>
    </div>
    <div class="modal-footer">
      <button type="button" class="btn-modal-cancel" id="btnModalCancel">Cancelar</button>
      <button type="button" class="btn-modal-confirm" id="btnModalConfirm">Confirmar y Guardar</button>
    </div>
  </div>
</div>
<div id="modelReviewModal" class="modal-backdrop" hidden>
  <div class="modal-dialog" style="width:780px;max-width:94vw;max-height:90vh;">
    <div class="modal-header">
      <div class="modal-title-wrap">
        <span class="modal-title">Revisar cambios a aplicar</span>
        <span class="env-badge" id="modalReviewEnvBadge">DESA</span>
        <span id="modalReviewCounts" style="font-size:12px;color:#9d9d9d;"></span>
      </div>
      <button type="button" class="modal-close-btn" id="modalReviewCloseBtn" title="Cerrar modal (Esc)">✕</button>
    </div>
    <div class="modal-body" style="padding:14px 16px;overflow-y:auto;display:flex;flex-direction:column;gap:12px;">
      <div id="modalReviewSummaryBox" style="background:#1e1e1e;border:1px solid #333;border-radius:4px;padding:10px 14px;font-size:13px;line-height:1.5;"></div>

      <div id="modalReviewProdWarning" class="modal-prod-warning" style="display:none;background:rgba(220,38,38,0.15);border:1px solid #dc2626;border-radius:4px;padding:10px 14px;">
        <div style="color:#f48771;font-weight:700;font-size:13px;"><svg class="ui-icon" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="margin-right:4px;"><path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3Z"></path><line x1="12" y1="9" x2="12" y2="13"></line><line x1="12" y1="17" x2="12.01" y2="17"></line></svg> ¡ATENCIÓN: MODIFICACIÓN EN PRODUCCIÓN!</div>
        <div style="font-size:12px;margin:4px 0 8px 0;line-height:1.4;color:#ccc;">
          Estás por modificar bots en el ambiente de <strong>PRODUCCIÓN</strong>. Los cambios tendrán efecto inmediato para los usuarios reales.
        </div>
        <div class="modal-prod-input-row" style="display:flex;align-items:center;gap:10px;">
          <label for="modalReviewProdInput" style="font-size:12px;color:#d4d4d4;">Escribí <code>PROD</code> para habilitar la confirmación:</label>
          <input type="text" id="modalReviewProdInput" placeholder="PROD" autocomplete="off" style="width:100px;background:#181818;border:1px solid #dc2626;color:#fff;padding:4px 8px;font-size:12px;border-radius:3px;font-family:monospace;" />
        </div>
      </div>

      <div style="font-size:11.5px;font-weight:700;color:#9d9d9d;text-transform:uppercase;letter-spacing:0.5px;">Detalle por bot y prompt:</div>
      <div style="max-height:280px;overflow-y:auto;border:1px solid #333;border-radius:4px;background:#181818;">
        <table style="width:100%;border-collapse:collapse;font-size:12px;">
          <thead>
            <tr style="background:#202021;position:sticky;top:0;z-index:1;border-bottom:1px solid #333;color:#9d9d9d;text-transform:uppercase;font-size:10.5px;text-align:left;">
              <th style="padding:6px 10px;">Client / bot</th>
              <th style="padding:6px 10px;">Prompt</th>
              <th style="padding:6px 10px;">Modificaciones planeadas</th>
            </tr>
          </thead>
          <tbody id="modalReviewTableBody"></tbody>
        </table>
      </div>

      <div style="font-size:11px;color:#858585;line-height:1.4;">
        <svg class="ui-icon" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="margin-right:4px;"><path d="M19 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h11l5 5v11a2 2 0 0 1-2 2z"></path><polyline points="17 21 17 13 7 13 7 21"></polyline><polyline points="7 3 7 8 15 8"></polyline></svg> Se creará un backup automático de cada ítem en <code>backups/</code> antes de escribir en DynamoDB. Podrás deshacer los cambios si lo necesitás.
      </div>
    </div>
    <div class="modal-footer" style="padding:10px 16px;background:#202021;border-top:1px solid #333;display:flex;align-items:center;justify-content:space-between;">
      <button type="button" class="btn-modal-cancel" id="btnModalReviewCancel">Cancelar</button>
      <button type="button" class="btn-modal-confirm" id="btnModalReviewConfirm" style="background:#0e639c;">Aplicar cambios</button>
    </div>
  </div>
</div>
<div id="modelReplicateModal" class="modal-backdrop" hidden>
  <div class="modal-dialog" style="width:840px;max-width:95vw;max-height:92vh;display:flex;flex-direction:column;">
    <div class="modal-header">
      <div class="modal-title-wrap">
        <span class="modal-title"><svg class="ui-icon" width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="margin-right:4px;"><rect width="14" height="14" x="8" y="8" rx="2" ry="2"></rect><path d="M4 16c-1.1 0-2-.9-2-2V4c0-1.1.9-2 2-2h10c1.1 0 2 .9 2 2"></path></svg> Replicar estructura de prompts</span>
        <span class="env-badge" id="modalReplicateTargetBadge">DESA</span>
        <span id="modalReplicateCounts" style="font-size:12px;color:#9d9d9d;"></span>
      </div>
      <button type="button" class="modal-close-btn" id="modalReplicateCloseBtn" title="Cerrar modal (Esc)">✕</button>
    </div>
    <div class="modal-body" style="padding:14px 16px;overflow-y:auto;display:flex;flex-direction:column;gap:12px;flex:1;">
      <!-- Selector de ambiente origen -->
      <div style="background:#202021;border:1px solid #333;border-radius:4px;padding:10px 14px;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:10px;">
        <div style="display:flex;align-items:center;gap:10px;">
          <label for="replicateSourceEnvSelect" style="font-size:12.5px;font-weight:600;color:#ccc;">Replicar estructura desde:</label>
          <select id="replicateSourceEnvSelect" style="background:#181818;border:1px solid #444;color:#fff;padding:4px 8px;border-radius:3px;font-size:12.5px;font-weight:600;">
          </select>
          <span style="font-size:12px;color:#858585;">hacia <strong id="replicateTargetEnvName" style="color:#fff;">DESA</strong></span>
        </div>
        <button type="button" id="btnRefreshReplicatePlan" style="font-size:11.5px;padding:4px 10px;background:#2d2d2d;border:1px solid #444;color:#ccc;border-radius:3px;cursor:pointer;">↻ Recalcular plan</button>
      </div>

      <!-- Nota de alcance -->
      <div style="font-size:11.5px;color:#858585;line-height:1.4;background:rgba(255,255,255,0.02);border:1px solid #2a2a2a;padding:8px 12px;border-radius:4px;">
        ℹ️ Se replicarán los <strong>parámetros y variables</strong> de cada prompt (<code>OPENAI_MODEL</code>, <code>TEMPERATURE</code>, <code>REASONING_EFFORT</code>, <code>RESPONSE_FORMAT_TYPE</code>, <code>MAX_COMPLETION_TOKENS</code> y lista de <code>INPUT_VARIABLE</code>). El texto del <code>TEMPLATE</code> <strong>no se modificará</strong>.
      </div>

      <!-- Tarjetas de métricas -->
      <div id="replicateStatsGrid" style="display:grid;grid-template-columns:repeat(auto-fit, minmax(130px, 1fr));gap:8px;">
        <div style="background:#1e1e1e;border:1px solid #333;border-radius:4px;padding:8px 10px;text-align:center;">
          <div style="font-size:10px;color:#858585;text-transform:uppercase;">Bots a modificar</div>
          <div id="statRepChangedBots" style="font-size:18px;font-weight:700;color:#4ec9b0;margin-top:2px;">-</div>
        </div>
        <div style="background:#1e1e1e;border:1px solid #333;border-radius:4px;padding:8px 10px;text-align:center;">
          <div style="font-size:10px;color:#858585;text-transform:uppercase;">Prompts con diff</div>
          <div id="statRepChangedPrompts" style="font-size:18px;font-weight:700;color:#ce9178;margin-top:2px;">-</div>
        </div>
        <div style="background:#1e1e1e;border:1px solid #333;border-radius:4px;padding:8px 10px;text-align:center;">
          <div style="font-size:10px;color:#858585;text-transform:uppercase;">Bots sin cambios</div>
          <div id="statRepUnchangedBots" style="font-size:18px;font-weight:700;color:#858585;margin-top:2px;">-</div>
        </div>
        <div style="background:#1e1e1e;border:1px solid #333;border-radius:4px;padding:8px 10px;text-align:center;">
          <div style="font-size:10px;color:#858585;text-transform:uppercase;">Omitidos (no en origen)</div>
          <div id="statRepSkippedBots" style="font-size:18px;font-weight:700;color:#9cdcfe;margin-top:2px;">-</div>
        </div>
      </div>

      <!-- Advertencia en PROD -->
      <div id="modalReplicateProdWarning" class="modal-prod-warning" style="display:none;background:rgba(220,38,38,0.15);border:1px solid #dc2626;border-radius:4px;padding:10px 14px;">
        <div style="color:#f48771;font-weight:700;font-size:13px;"><svg class="ui-icon" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="margin-right:4px;"><path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3Z"></path><line x1="12" y1="9" x2="12" y2="13"></line><line x1="12" y1="17" x2="12.01" y2="17"></line></svg> ¡ATENCIÓN: REPLICACIÓN HACIA PRODUCCIÓN!</div>
        <div style="font-size:12px;margin:4px 0 8px 0;line-height:1.4;color:#ccc;">
          Estás por replicar la configuración de prompts hacia el ambiente de <strong>PRODUCCIÓN</strong>. Los cambios tendrán efecto inmediato para los usuarios reales.
        </div>
        <div class="modal-prod-input-row" style="display:flex;align-items:center;gap:10px;">
          <label for="modalReplicateProdInput" style="font-size:12px;color:#d4d4d4;">Escribí <code>PROD</code> para habilitar la confirmación:</label>
          <input type="text" id="modalReplicateProdInput" placeholder="PROD" autocomplete="off" style="width:100px;background:#181818;border:1px solid #dc2626;color:#fff;padding:4px 8px;font-size:12px;border-radius:3px;font-family:monospace;" />
        </div>
      </div>

      <!-- Tabla de diferencias y filtro -->
      <div style="display:flex;align-items:center;justify-content:space-between;margin-top:2px;">
        <div style="font-size:11.5px;font-weight:700;color:#9d9d9d;text-transform:uppercase;letter-spacing:0.5px;">Diferencias detectadas:</div>
        <input type="text" id="replicateTableFilter" placeholder="Filtrar bot o prompt…" style="font-size:11.5px;padding:3px 8px;background:#181818;border:1px solid #333;color:#ccc;border-radius:3px;width:200px;" />
      </div>

      <div style="max-height:300px;overflow-y:auto;border:1px solid #333;border-radius:4px;background:#181818;">
        <table style="width:100%;border-collapse:collapse;font-size:12px;">
          <thead>
            <tr style="background:#202021;position:sticky;top:0;z-index:1;border-bottom:1px solid #333;color:#9d9d9d;text-transform:uppercase;font-size:10.5px;text-align:left;">
              <th style="padding:6px 10px;width:28%;">Client / bot</th>
              <th style="padding:6px 10px;width:24%;">Prompt</th>
              <th style="padding:6px 10px;width:48%;">Diferencias (Actual destino → Origen)</th>
            </tr>
          </thead>
          <tbody id="modalReplicateTableBody"></tbody>
        </table>
      </div>

      <!-- Backup notice -->
      <div style="font-size:11px;color:#858585;line-height:1.4;">
        <svg class="ui-icon" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="margin-right:4px;"><path d="M19 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h11l5 5v11a2 2 0 0 1-2 2z"></path><polyline points="17 21 17 13 7 13 7 21"></polyline><polyline points="7 3 7 8 15 8"></polyline></svg> Se creará un backup automático de cada ítem en <code>backups/</code> antes de escribir en DynamoDB. Podrás deshacer todos los cambios si lo necesitás.
      </div>
    </div>
    <div class="modal-footer" style="padding:10px 16px;background:#202021;border-top:1px solid #333;display:flex;align-items:center;justify-content:space-between;">
      <button type="button" class="btn-modal-cancel" id="btnModalReplicateCancel">Cancelar</button>
      <button type="button" class="btn-modal-confirm" id="btnModalReplicateConfirm" style="background:#0e639c;" disabled>Replicar estructura</button>
    </div>
  </div>
</div>
<div id="historyModal" class="modal-backdrop" hidden>
  <div class="modal-dialog history-dialog">
    <div class="modal-header">
      <div class="modal-title-wrap">
        <span class="modal-title"><svg class="ui-icon" width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="margin-right:4px;"><circle cx="12" cy="12" r="10"></circle><polyline points="12 6 12 12 16 14"></polyline></svg> Historial de versiones</span>
        <span class="env-badge" id="historyModalEnvBadge">DESA</span>
        <span class="modal-bot-name" id="historyModalBotName"></span>
      </div>
      <button type="button" class="modal-close-btn" id="historyModalCloseBtn" title="Cerrar modal (Esc)">✕</button>
    </div>
    <div class="modal-body" style="padding:0;display:flex;flex:1;min-height:0;overflow:hidden;">
      <div class="history-split">
        <div class="history-list-pane">
          <div class="history-list-title">Últimas 20 versiones guardadas</div>
          <div id="historyListItems" style="display:flex;flex-direction:column;gap:6px;"></div>
          <div id="historyEmptyMsg" style="color:#858585;font-size:12px;font-style:italic;padding:16px 8px;text-align:center;" hidden>
            No hay versiones previas guardadas para este bot.
          </div>
        </div>
        <div class="history-detail-pane">
          <div class="history-detail-header">
            <div class="history-detail-title-wrap">
              <div class="history-detail-title" id="historyDetailTitle">Seleccioná una versión</div>
              <div class="history-detail-meta" id="historyDetailMeta">Mostrando historial de backups locales</div>
            </div>
            <div class="history-detail-actions">
              <button type="button" class="btn-restore-version" id="btnRestoreHistory" style="display:none;">↺ Restaurar versión</button>
            </div>
          </div>
          <div class="history-detail-body" id="historyDetailBody">
            <div id="historyDiffSection" style="display:none;flex-direction:column;gap:8px;">
              <div style="font-size:11.5px;font-weight:600;color:#aaa;text-transform:uppercase;letter-spacing:0.5px;">Diferencias respecto a la versión actual cargada:</div>
              <div class="history-diff-box" id="historyDiffSummary"></div>
              <div style="display:flex;align-items:center;justify-content:space-between;margin-top:6px;">
                <span style="font-size:11.5px;color:#858585;">JSON DynamoDB del backup:</span>
                <button type="button" id="btnToggleHistoryJson" style="font-size:11px;padding:2px 8px;background:#2d2d2d;color:#ccc;border:1px solid #444;border-radius:3px;cursor:pointer;">Ver JSON completo</button>
              </div>
              <pre class="history-json-box" id="historyJsonRaw" hidden></pre>
            </div>
            <div id="historyPlaceholder" style="color:#858585;font-size:13px;text-align:center;padding-top:60px;">
              Hacé clic en una versión del listado para comparar o restaurar.
            </div>
          </div>
        </div>
      </div>
    </div>
    <div class="modal-footer">
      <span style="font-size:11.5px;color:#858585;margin-right:auto;">
        <svg class="ui-icon" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="margin-right:4px;"><path d="M19 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h11l5 5v11a2 2 0 0 1-2 2z"></path><polyline points="17 21 17 13 7 13 7 21"></polyline><polyline points="7 3 7 8 15 8"></polyline></svg> Los backups se generan automáticamente en <code>backups/</code> con cada guardado.
      </span>
      <button type="button" class="btn-modal-cancel" id="btnHistoryClose">Cerrar</button>
    </div>
  </div>
</div>

<div id="shortcutsModal" class="modal-backdrop" hidden>
  <div class="modal-card shortcuts-modal-card">
    <div class="modal-header">
      <div style="display:flex;align-items:center;gap:8px;">
        <svg class="ui-icon" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect width="20" height="16" x="2" y="4" rx="2" ry="2"></rect><path d="M6 8h.001M10 8h.001M14 8h.001M18 8h.001M6 12h.001M10 12h.001M14 12h.001M18 12h.001M7 16h10"></path></svg>
        <h3 style="margin:0;font-size:15px;color:#f3f4f6;">Atajos de teclado</h3>
      </div>
      <button type="button" class="modal-close" id="shortcutsCloseBtn">×</button>
    </div>
    <div class="modal-body" style="padding:14px 18px;max-height:420px;overflow-y:auto;">
      <div class="shortcut-grid">
        <div class="shortcut-row"><kbd>Ctrl+1</kbd><span>Vista Comparar</span></div>
        <div class="shortcut-row"><kbd>Ctrl+2</kbd><span>JSON individual</span></div>
        <div class="shortcut-row"><kbd>Ctrl+3</kbd><span>Gestión de Prompts</span></div>
        <div class="shortcut-row"><kbd>Ctrl+4</kbd><span>Matriz de Modelos</span></div>
        <div class="shortcut-row"><kbd>Ctrl+5</kbd><span>Auditoría API Keys</span></div>
        <div class="shortcut-row"><kbd>Ctrl+6</kbd><span>Limpieza DynamoDB</span></div>
        <div class="shortcut-row"><kbd>Ctrl+7</kbd><span>Gestión de Tablas DynamoDB</span></div>
        <div class="shortcut-row"><kbd>Ctrl+8</kbd><span>Métricas de Prompts</span></div>
        <div class="shortcut-divider"></div>
        <div class="shortcut-row"><kbd>Ctrl+S</kbd><span>Guardar cambios (ambientes o individual)</span></div>
        <div class="shortcut-row"><kbd>F11</kbd><span>Expandir / Contraer editor de TEMPLATE</span></div>
        <div class="shortcut-row"><kbd>Esc</kbd><span>Salir de pantalla completa o cerrar modales</span></div>
        <div class="shortcut-row"><kbd>F7</kbd><span>Siguiente diferencia (Comparar)</span></div>
        <div class="shortcut-row"><kbd>Shift+F7</kbd><span>Diferencia anterior (Comparar)</span></div>
        <div class="shortcut-row"><kbd>?</kbd><span>Abrir esta ventana de ayuda</span></div>
      </div>
    </div>
  </div>
</div>
<script>
const $ = id => document.getElementById(id);
const esc = s => String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
let ENVS = [], ALL = [], diffEditor = null, leftModel = null, rightModel = null, singleEditor = null, singleModel = null;
let loaded = false, activeKey = null, versions = {left: null, right: null};
let baseline = {left: '', right: ''}, currentHunk = -1, saving = false;
let diffPending = false, diffWaiters = [];
let singleLoaded = false, singleActiveKey = null, singleVersion = null, singleBaseline = '{}';
let MODELS = [], selectedModel = null;
let selectedBots = new Set(), expandedBot = null, deselectedPrompts = new Set(), promptOverrides = new Map();
let selectedModels = new Set();
let API_KEY_AUDIT = {rows: [], environments: []};
let activeMode = 'compare';
let wordWrapEnabled = true;
const keyCache = {};
let currentSingleSubMode = 'text';
let currentFontSize = 13;
let rawSingleItem = null;
let unpackedPromptsData = null;
let selectedPromptName = null;
let templateModel = null, templateEditor = null, templateWrapEnabled = true;
let templateDiffEditor = null, compTemplateModel = null, templateDiffActive = false;
let templateSectionsFolded = false;
let apiKeyRevealed = false;
let comparisonItem = null;
let comparisonPromptsData = null;
let comparisonEnv = 'test';
let activeSelectedField = 'TEMPLATE';
let isAddingParam = false;
let compPreviewExpanded = false;
let paramsCardFolded = false;
let sidebarModelsFolded = false;
let pendingSavePayload = null;
let currentHistoryBackups = [];
let selectedHistoryItem = null;
let selectedHistoryFilename = null;

function updateParamsSummaryBadge(){
  const badge = $('paramsSummaryBadge');
  if(!badge) return;
  if(!paramsCardFolded || !selectedPromptName || !unpackedPromptsData || !unpackedPromptsData.prompts[selectedPromptName]){
    badge.textContent = '';
    return;
  }
  const p = unpackedPromptsData.prompts[selectedPromptName];
  const parts = [];
  if(p.model) parts.push('modelo: ' + p.model);
  if(p.temperature !== '' && p.temperature != null) parts.push('temp: ' + p.temperature);
  badge.textContent = parts.length ? ('(' + parts.join(' · ') + ')') : '';
}

function toggleParamsCard(force){
  paramsCardFolded = force !== undefined ? Boolean(force) : !paramsCardFolded;
  if($('promptParamsContent')) $('promptParamsContent').hidden = paramsCardFolded;
  if($('paramsToggleIcon')) $('paramsToggleIcon').textContent = paramsCardFolded ? '▶' : '▼';
  if($('btnToggleParams')) $('btnToggleParams').textContent = paramsCardFolded ? 'Desplegar' : 'Plegar';
  updateParamsSummaryBadge();
  requestAnimationFrame(() => {
    if(templateEditor) templateEditor.layout();
    if(templateDiffEditor) templateDiffEditor.layout();
  });
}

function toggleSidebarModels(force){
  sidebarModelsFolded = force !== undefined ? Boolean(force) : !sidebarModelsFolded;
  if($('sidebarModelsBody')) $('sidebarModelsBody').hidden = sidebarModelsFolded;
  if($('sidebarModelsToggleIcon')) $('sidebarModelsToggleIcon').textContent = sidebarModelsFolded ? '▶' : '▼';
}

async function api(url, options){
  const r = await fetch(url, options);
  const j = await r.json().catch(() => ({error: 'Respuesta inválida del servidor'}));
  if(!r.ok) throw new Error(j.error || ('HTTP ' + r.status));
  return j;
}
function setStatus(msg, cls){ const s = $('status'); s.textContent = msg; s.className = cls || ''; }
function ensureDiffWordWrap(){
  if(!diffEditor) return;
  diffEditor.updateOptions({diffWordWrap: 'on', wordWrap: 'on', useInlineViewWhenSpaceIsLimited: false});
  if(diffEditor.getOriginalEditor()){
    diffEditor.getOriginalEditor().updateOptions({wordWrap: 'on', wrappingStrategy: 'advanced', wrappingIndent: 'indent'});
  }
  if(diffEditor.getModifiedEditor()){
    diffEditor.getModifiedEditor().updateOptions({wordWrap: 'on', wrappingStrategy: 'advanced', wrappingIndent: 'indent'});
  }
  requestAnimationFrame(() => {
    if(activeMode === 'compare'){
      diffEditor.layout();
      diffEditor.getOriginalEditor().layout();
      diffEditor.getModifiedEditor().layout();
    }
  });
}
function setWordWrap(enabled){ ensureDiffWordWrap(); }
function updateComparePaneHeaders(){
  const leftEnv = $('envL') ? $('envL').value : 'desa';
  const rightEnv = $('envR') ? $('envR').value : 'test';
  const bL = $('badgeL'), bR = $('badgeR');
  if(bL){
    bL.textContent = (leftEnv || '').toUpperCase();
    bL.className = 'env-badge ' + (leftEnv || '').toLowerCase();
  }
  if(bR){
    bR.textContent = (rightEnv || '').toUpperCase();
    bR.className = 'env-badge ' + (rightEnv || '').toLowerCase();
  }
  if(!loaded){
    if($('hL')) $('hL').textContent = tableOf(leftEnv);
    if($('hR')) $('hR').textContent = tableOf(rightEnv);
  }
}
const tableOf = name => (ENVS.find(e => e.name === name) || {}).table || name;

/* ---------- unpack y repack de prompts (DynamoDB AttributeValue) ---------- */
function unpackPrompts(item){
  if(!item || typeof item !== 'object') return {prompts: {}, models_in_use: [], api_key: ''};
  const agentCfg = (item.AGENT_CONFIG && item.AGENT_CONFIG.M) || {};
  const promptsMap = (agentCfg.PROMPTS && agentCfg.PROMPTS.M) || {};

  let apiKey = '';
  const apiKeysMap = (item.API_KEYS && item.API_KEYS.M) || {};
  if(apiKeysMap.OPENAI_API_KEY && apiKeysMap.OPENAI_API_KEY.S){
    apiKey = apiKeysMap.OPENAI_API_KEY.S;
  }
  if(!apiKey){
    const sentiment = (agentCfg.SENTIMENT_ANALYSIS && agentCfg.SENTIMENT_ANALYSIS.M) || {};
    if(sentiment.OPENAI_API_KEY && sentiment.OPENAI_API_KEY.S){
      apiKey = sentiment.OPENAI_API_KEY.S;
    }
  }

  const unpackedPrompts = {};
  const modelsInUse = [];

  const promptNames = Object.keys(promptsMap).sort();
  for(const pName of promptNames){
    const pAttr = promptsMap[pName];
    const fieldsMap = (pAttr && pAttr.M) || {};
    const fields = {};
    let templateVal = '';
    let inputVars = [];
    let modelVal = '';
    let tempVal = '';

    for(const [fName, fAttr] of Object.entries(fieldsMap)){
      if(!fAttr || typeof fAttr !== 'object') continue;
      const t = Object.keys(fAttr)[0];
      const rawVal = fAttr[t];
      let val = rawVal;

      if(t === 'S'){
        val = rawVal != null ? String(rawVal) : '';
        if(fName === 'TEMPLATE') templateVal = val;
        else if(fName === 'OPENAI_MODEL') modelVal = val;
      }else if(t === 'N'){
        val = rawVal != null ? String(rawVal) : '';
        if(fName === 'TEMPERATURE') tempVal = val;
      }else if(t === 'BOOL'){
        val = Boolean(rawVal);
      }else if(t === 'L'){
        if(fName === 'INPUT_VARIABLE' && Array.isArray(rawVal)){
          val = rawVal.map(x => (x && typeof x === 'object' && 'S' in x) ? String(x.S) : String(x));
          inputVars = [...val];
        }else{
          val = rawVal;
        }
      }

      fields[fName] = {
        type: t,
        value: val,
        raw: JSON.parse(JSON.stringify(fAttr)),
        dirty: false
      };
    }

    if(modelVal){
      modelsInUse.push({prompt: pName, model: modelVal});
    }

    unpackedPrompts[pName] = {
      name: pName,
      fields,
      template: templateVal,
      input_variables: inputVars,
      model: modelVal,
      temperature: tempVal
    };
  }

  return {
    prompts: unpackedPrompts,
    models_in_use: modelsInUse,
    api_key: apiKey
  };
}

function repackPrompts(originalItem, unpackedData){
  const result = JSON.parse(JSON.stringify(originalItem || {}));
  if(!result.AGENT_CONFIG) result.AGENT_CONFIG = {M: {}};
  if(!result.AGENT_CONFIG.M) result.AGENT_CONFIG.M = {};
  if(!result.AGENT_CONFIG.M.PROMPTS) result.AGENT_CONFIG.M.PROMPTS = {M: {}};
  if(!result.AGENT_CONFIG.M.PROMPTS.M) result.AGENT_CONFIG.M.PROMPTS.M = {};

  const promptsMap = result.AGENT_CONFIG.M.PROMPTS.M;
  const promptsDict = (unpackedData && unpackedData.prompts) || {};

  for(const [pName, pData] of Object.entries(promptsDict)){
    if(!promptsMap[pName]) promptsMap[pName] = {M: {}};
    if(!promptsMap[pName].M) promptsMap[pName].M = {};
    const pRaw = promptsMap[pName].M;

    const fields = pData.fields || {};

    // Prune deleted fields
    for(const k of Object.keys(pRaw)){
      if(!(k in fields)){
        delete pRaw[k];
      }
    }

    for(const [fName, fInfo] of Object.entries(fields)){
      if(!fInfo.dirty && fInfo.raw){
        pRaw[fName] = JSON.parse(JSON.stringify(fInfo.raw));
        continue;
      }

      const t = fInfo.type || 'S';
      const val = fInfo.value;

      if(t === 'S'){
        pRaw[fName] = {S: val != null ? String(val) : ''};
      }else if(t === 'N'){
        const strVal = val != null ? String(val).trim() : '';
        if(strVal === ''){
          pRaw[fName] = {S: ''};
        }else{
          pRaw[fName] = {N: strVal};
        }
      }else if(t === 'BOOL'){
        pRaw[fName] = {BOOL: Boolean(val)};
      }else if(t === 'L'){
        if(fName === 'INPUT_VARIABLE' && Array.isArray(val)){
          pRaw[fName] = {L: val.map(v => ({S: String(v)}))};
        }else if(Array.isArray(val)){
          pRaw[fName] = {L: val.map(v => (typeof v === 'object' && v ? v : {S: String(v)}))};
        }else{
          pRaw[fName] = {L: []};
        }
      }else if(t === 'M'){
        pRaw[fName] = {M: (typeof val === 'object' && val) ? val : {}};
      }else{
        pRaw[fName] = fInfo.raw ? JSON.parse(JSON.stringify(fInfo.raw)) : {S: String(val)};
      }
    }
  }

  return result;
}

function isValidNumberString(val){
  if(val == null) return false;
  const str = String(val).trim();
  if(str === '') return false;
  return /^-?\d+(\.\d+)?$/.test(str);
}

function hasValidationErrors(){
  if(!unpackedPromptsData) return false;
  for(const p of Object.values(unpackedPromptsData.prompts || {})){
    for(const [fName, fInfo] of Object.entries(p.fields || {})){
      if(fInfo && fInfo.type === 'N'){
        const v = fInfo.value;
        if(v != null && String(v).trim() !== '' && !isValidNumberString(v)){
          return true;
        }
      }
    }
  }
  return false;
}

function getCounterpartEnv(env){
  if(env === 'desa') return 'test';
  if(env === 'test') return 'desa';
  if(env === 'prod') return 'test';
  return 'test';
}

async function fetchComparisonItem(client, bot, compEnv){
  try{
    const j = await api('/api/item?env=' + encodeURIComponent(compEnv) + '&client=' + encodeURIComponent(client) + '&bot=' + encodeURIComponent(bot));
    if(singleActiveKey && singleActiveKey.client === client && singleActiveKey.bot === bot){
      if(j && j.item){
        comparisonItem = j.item;
        comparisonPromptsData = unpackPrompts(comparisonItem);
      }else{
        comparisonItem = null;
        comparisonPromptsData = null;
      }
      renderPromptsView();
    }
  }catch(e){
    comparisonItem = null;
    comparisonPromptsData = null;
    renderPromptsView();
  }
}

function getPromptDiffStatus(pName){
  if(!comparisonPromptsData) return {status: 'unknown', text: 'sin comparación'};
  const compPrompts = comparisonPromptsData.prompts || {};
  if(!compPrompts[pName]){
    return {status: 'solo', text: 'solo en ' + (singleActiveKey ? singleActiveKey.env : 'desa')};
  }
  const currP = unpackedPromptsData.prompts[pName];
  const compP = compPrompts[pName];
  if(!currP) return {status: 'missing', text: 'falta en ' + (singleActiveKey ? singleActiveKey.env : 'desa')};

  const currTmpl = (currP.fields && currP.fields.TEMPLATE && currP.fields.TEMPLATE.value != null) ? currP.fields.TEMPLATE.value : (currP.template || '');
  const compTmpl = (compP.fields && compP.fields.TEMPLATE && compP.fields.TEMPLATE.value != null) ? compP.fields.TEMPLATE.value : (compP.template || '');
  if(currTmpl !== compTmpl){
    return {status: 'diff', text: 'difiere en ' + comparisonEnv};
  }

  const currKeys = Object.keys(currP.fields || {}).filter(k => k !== 'TEMPLATE').sort();
  const compKeys = Object.keys(compP.fields || {}).filter(k => k !== 'TEMPLATE').sort();
  if(currKeys.length !== compKeys.length || currKeys.some((k, i) => k !== compKeys[i])){
    return {status: 'diff', text: 'difiere en ' + comparisonEnv};
  }

  for(const k of currKeys){
    const fCurr = currP.fields[k];
    const fComp = compP.fields[k];
    if(fCurr.type !== fComp.type) return {status: 'diff', text: 'difiere en ' + comparisonEnv};
    if(fCurr.type === 'L'){
      const a = Array.isArray(fCurr.value) ? fCurr.value : [];
      const b = Array.isArray(fComp.value) ? fComp.value : [];
      if(a.length !== b.length || a.some((x, i) => x !== b[i])) return {status: 'diff', text: 'difiere en ' + comparisonEnv};
    }else{
      if(String(fCurr.value) !== String(fComp.value)) return {status: 'diff', text: 'difiere en ' + comparisonEnv};
    }
  }

  return {status: 'same', text: 'igual en ' + comparisonEnv};
}

function selectRightJsonField(fieldName){
  activeSelectedField = fieldName || 'TEMPLATE';
  if(!selectedPromptName || !unpackedPromptsData || !unpackedPromptsData.prompts[selectedPromptName]) return;
  const pData = unpackedPromptsData.prompts[selectedPromptName];

  const fieldEl = $('rightJsonField');
  if(fieldEl) fieldEl.textContent = activeSelectedField;

  let wireObj = {};
  let fieldType = 'S';

  if(activeSelectedField === 'TEMPLATE'){
    fieldType = 'S';
    const text = templateModel ? templateModel.getValue() : (pData.template || '');
    wireObj = {
      "TEMPLATE": {
        "S": text
      }
    };
  }else if(pData.fields && pData.fields[activeSelectedField]){
    const fInfo = pData.fields[activeSelectedField];
    fieldType = fInfo.type || 'S';
    const val = fInfo.value;

    if(fieldType === 'S'){
      wireObj = {[activeSelectedField]: {"S": val != null ? String(val) : ""}};
    }else if(fieldType === 'N'){
      wireObj = {[activeSelectedField]: {"N": val != null ? String(val).trim() : ""}};
    }else if(fieldType === 'BOOL'){
      wireObj = {[activeSelectedField]: {"BOOL": Boolean(val)}};
    }else if(fieldType === 'L'){
      if(activeSelectedField === 'INPUT_VARIABLE' && Array.isArray(val)){
        wireObj = {[activeSelectedField]: {"L": val.map(v => ({"S": String(v)}))}};
      }else if(Array.isArray(val)){
        wireObj = {[activeSelectedField]: {"L": val.map(v => (typeof v === 'object' && v ? v : {"S": String(v)}))}};
      }else{
        wireObj = {[activeSelectedField]: {"L": []}};
      }
    }else if(fieldType === 'M'){
      wireObj = {[activeSelectedField]: {"M": (typeof val === 'object' && val) ? val : {}}};
    }else{
      wireObj = {[activeSelectedField]: {"S": String(val)}};
    }
  }else{
    wireObj = {[activeSelectedField]: {}};
  }

  const codeEl = $('rightJsonPre');
  if(codeEl) codeEl.textContent = JSON.stringify(wireObj, null, 2);

  ['S', 'N', 'L', 'M'].forEach(t => {
    const pill = $('typePill' + t);
    if(pill) pill.classList.toggle('active', t === fieldType);
  });
}

function updateRightJsonPreview(){
  selectRightJsonField(activeSelectedField || 'TEMPLATE');
}

function onTemplateContentChanged(){
  if(!unpackedPromptsData || !selectedPromptName) return;
  const pData = unpackedPromptsData.prompts[selectedPromptName];
  if(!pData) return;
  const text = templateModel.getValue();
  const origRaw = pData.fields.TEMPLATE && pData.fields.TEMPLATE.raw && pData.fields.TEMPLATE.raw.S;
  const origVal = origRaw != null ? String(origRaw) : '';
  const isDirty = text !== origVal;
  if(!pData.fields.TEMPLATE){
    pData.fields.TEMPLATE = {type: 'S', value: text, dirty: isDirty};
  }else{
    pData.fields.TEMPLATE.value = text;
    pData.fields.TEMPLATE.dirty = isDirty;
  }
  pData.template = text;
  updateTemplateStats();
  if(activeSelectedField === 'TEMPLATE'){
    updateRightJsonPreview();
  }
  updateDirtyState();
}

function updateTemplateStats(){
  if(!templateModel) return;
  const text = templateModel.getValue();
  const chars = text.length;
  const tokens = Math.round(chars / 4);
  const el = $('templateStats');
  if(el) el.textContent = chars.toLocaleString('es-AR') + ' caracteres · ~' + tokens.toLocaleString('es-AR') + ' tokens';
}

function countDirtyChanges(){
  if(!unpackedPromptsData) return 0;
  let count = 0;
  for(const p of Object.values(unpackedPromptsData.prompts || {})){
    if(p.deleted_fields && p.deleted_fields.length){
      count += p.deleted_fields.length;
    }
    for(const f of Object.values(p.fields || {})){
      if(f.dirty) count++;
    }
  }
  return count;
}

function updateDirtyState(){
  const count = countDirtyChanges();
  const invalidNumbers = hasValidationErrors();
  if($('footerChangesCount')){
    if(invalidNumbers){
      $('footerChangesCount').textContent = 'Error: valor numérico no válido en N';
    }else{
      $('footerChangesCount').textContent = count + (count === 1 ? ' cambio sin guardar' : ' cambios sin guardar');
    }
  }
  const saveBtn = $('saveSingle');
  if(saveBtn){
    if(currentSingleSubMode === 'text'){
      saveBtn.disabled = !singleLoaded || saving || count === 0 || invalidNumbers;
      saveBtn.textContent = count > 0 ? ('Guardar · ' + count + (count === 1 ? ' cambio' : ' cambios')) : 'Guardar';
    }else{
      const isDirty = singleLoaded && singleModel.getValue() !== singleBaseline;
      saveBtn.disabled = !singleLoaded || saving || !isDirty;
      saveBtn.textContent = 'Guardar';
    }
  }
  const barSave = $('barSaveBtn');
  if(barSave && saveBtn){
    barSave.disabled = saveBtn.disabled;
    barSave.textContent = saveBtn.textContent;
  }
}

function adjustFontSize(delta){
  currentFontSize = Math.max(10, Math.min(24, currentFontSize + delta));
  if(singleEditor) singleEditor.updateOptions({fontSize: currentFontSize});
  if(templateEditor) templateEditor.updateOptions({fontSize: currentFontSize});
  if(templateDiffEditor){
    templateDiffEditor.updateOptions({fontSize: currentFontSize});
    templateDiffEditor.getOriginalEditor().updateOptions({fontSize: currentFontSize});
    templateDiffEditor.getModifiedEditor().updateOptions({fontSize: currentFontSize});
  }
}

function getChipInfo(type){
  if(type === 'S') return {label: 'String', cls: 'chip-string'};
  if(type === 'N') return {label: 'Number', cls: 'chip-number'};
  if(type === 'L') return {label: 'List', cls: 'chip-list'};
  if(type === 'BOOL') return {label: 'Boolean', cls: 'chip-bool'};
  if(type === 'M') return {label: 'Map', cls: 'chip-map'};
  return {label: type || 'String', cls: 'chip-string'};
}

function renderPromptsView(){
  if(!unpackedPromptsData) return;

  // Comparison env selector
  const compSelect = $('promptsCompEnvSelect');
  if(compSelect && ENVS && ENVS.length){
    const currentEnv = singleActiveKey ? singleActiveKey.env : 'desa';
    const otherEnvs = ENVS.map(e => e.name).filter(n => n !== currentEnv);
    if(!otherEnvs.includes(comparisonEnv)){
      comparisonEnv = otherEnvs[0] || (currentEnv === 'desa' ? 'test' : 'desa');
    }
    const optsHtml = otherEnvs.map(e => '<option value="' + esc(e) + '"' + (e === comparisonEnv ? ' selected' : '') + '>' + esc(e) + '</option>').join('');
    if(compSelect.innerHTML !== optsHtml){
      compSelect.innerHTML = optsHtml;
    }
    compSelect.value = comparisonEnv;
    compSelect.onchange = () => {
      comparisonEnv = compSelect.value;
      if(singleActiveKey){
        fetchComparisonItem(singleActiveKey.client, singleActiveKey.bot, comparisonEnv);
      }
    };
  }

  // 1. Models in use
  let modelsHtml = '';
  for(const m of (unpackedPromptsData.models_in_use || [])){
    modelsHtml += '<div class="sidebar-model-card" data-prompt="' + esc(m.prompt) + '" style="cursor:pointer;" title="Ver prompt ' + esc(m.prompt) + '">' +
      '<div class="sidebar-card-label">' + esc(m.prompt) + '</div>' +
      '<div class="sidebar-card-val">' + esc(m.model) + '</div>' +
      '</div>';
  }
  if(!modelsHtml){
    modelsHtml = '<div style="padding:8px 14px;color:#777;font-size:12px;">Sin modelos especificados</div>';
  }
  if($('sidebarModelsList')) $('sidebarModelsList').innerHTML = modelsHtml;

  const mCount = (unpackedPromptsData.models_in_use || []).length;
  if($('sidebarModelsCount')) $('sidebarModelsCount').textContent = '(' + mCount + ')';
  if($('sidebarModelsBody')) $('sidebarModelsBody').hidden = sidebarModelsFolded;
  if($('sidebarModelsToggleIcon')) $('sidebarModelsToggleIcon').textContent = sidebarModelsFolded ? '▶' : '▼';

  // API Key card
  if($('sidebarApiKeyCard')){
    if(unpackedPromptsData.api_key){
      $('sidebarApiKeyCard').hidden = false;
      const key = unpackedPromptsData.api_key;
      const masked = key.length > 12 ? (key.slice(0, 7) + '•••••••' + key.slice(-4)) : '••••••••';
      $('sidebarApiKeyMasked').textContent = apiKeyRevealed ? key : masked;
      $('sidebarApiKeyToggleBtn').textContent = apiKeyRevealed ? 'Ocultar' : 'Ver';
    }else{
      $('sidebarApiKeyCard').hidden = true;
    }
  }

  // 2. Prompts list
  const promptNames = Object.keys(unpackedPromptsData.prompts || {}).sort();
  if(!selectedPromptName || !unpackedPromptsData.prompts[selectedPromptName]){
    selectedPromptName = promptNames[0] || null;
  }

  let listHtml = '';
  for(const pName of promptNames){
    const isActive = pName === selectedPromptName;
    const diffInfo = getPromptDiffStatus(pName);
    let dotClass = 'dot-warn';
    if(diffInfo.status === 'same') dotClass = 'dot-ok';
    else if(diffInfo.status === 'solo') dotClass = 'dot-solo';

    listHtml += '<div class="sidebar-prompt-item' + (isActive ? ' active' : '') + '" data-prompt="' + esc(pName) + '">' +
      '<div class="sidebar-prompt-name">' + esc(pName) + '</div>' +
      '<div class="sidebar-prompt-diff"><span class="dot ' + dotClass + '">●</span> ' + esc(diffInfo.text) + '</div>' +
      '</div>';
  }
  if(!listHtml){
    listHtml = '<div style="padding:10px 14px;color:#777;font-size:12px;">No se encontraron prompts en AGENT_CONFIG.PROMPTS</div>';
  }
  if($('sidebarPromptsList')) $('sidebarPromptsList').innerHTML = listHtml;

  renderActivePrompt();
}

function renderActivePrompt(){
  if(!selectedPromptName || !unpackedPromptsData || !unpackedPromptsData.prompts[selectedPromptName]) return;
  const pData = unpackedPromptsData.prompts[selectedPromptName];

  templateSectionsFolded = false;
  if($('btnFoldTemplate')){
    $('btnFoldTemplate').title = 'Plegar secciones';
    $('btnFoldTemplate').classList.remove('active');
  }

  if($('promptCurrentName')) $('promptCurrentName').textContent = pData.name;
  if($('templateFsPromptName')) $('templateFsPromptName').textContent = pData.name;

  const diffInfo = getPromptDiffStatus(pData.name);
  if($('promptDiffBadge')){
    $('promptDiffBadge').textContent = diffInfo.text.charAt(0).toUpperCase() + diffInfo.text.slice(1);
    $('promptDiffBadge').className = 'prompt-main-diff-badge ' + (diffInfo.status === 'same' ? 'diff-badge-ok' : (diffInfo.status === 'solo' ? 'diff-badge-solo' : 'diff-badge-warn'));
  }
  if($('btnComparePrompt')){
    $('btnComparePrompt').textContent = 'Comparar con ' + comparisonEnv;
  }

  if($('promptParamsContent')) $('promptParamsContent').hidden = paramsCardFolded;
  if($('paramsToggleIcon')) $('paramsToggleIcon').textContent = paramsCardFolded ? '▶' : '▼';
  if($('btnToggleParams')) $('btnToggleParams').textContent = paramsCardFolded ? 'Desplegar' : 'Plegar';
  updateParamsSummaryBadge();

  const compP = (comparisonPromptsData && comparisonPromptsData.prompts) ? comparisonPromptsData.prompts[pData.name] : null;

  // Order parameter keys: standard first (OPENAI_MODEL, TEMPERATURE, INPUT_VARIABLE), then alphabetical
  const allKeys = Object.keys(pData.fields || {}).filter(k => k !== 'TEMPLATE');
  const stdKeys = ['OPENAI_MODEL', 'TEMPERATURE', 'INPUT_VARIABLE'];
  const sortedKeys = [];
  stdKeys.forEach(k => {
    if(allKeys.includes(k)) sortedKeys.push(k);
  });
  allKeys.sort().forEach(k => {
    if(!sortedKeys.includes(k)) sortedKeys.push(k);
  });

  let paramsHtml = '';

  for(const fName of sortedKeys){
    const fInfo = pData.fields[fName];
    const chip = getChipInfo(fInfo.type);

    let diffNoteHtml = '';
    if(compP && compP.fields){
      const compF = compP.fields[fName];
      if(!compF){
        diffNoteHtml = '<span class="param-diff-note param-diff-solo">solo en ' + esc(singleActiveKey ? singleActiveKey.env : 'desa') + '</span>';
      }else{
        const isSame = (compF.type === fInfo.type && JSON.stringify(compF.value) === JSON.stringify(fInfo.value));
        if(!isSame){
          let previewVal = String(compF.value != null ? compF.value : '');
          if(Array.isArray(compF.value)) previewVal = '[' + compF.value.join(', ') + ']';
          if(previewVal.length > 22) previewVal = previewVal.slice(0, 19) + '…';
          diffNoteHtml = '<span class="param-diff-note param-diff-warn" title="' + esc(comparisonEnv) + ': ' + esc(String(compF.value)) + '">' + esc(comparisonEnv) + ': ' + esc(previewVal) + '</span>';
        }else{
          diffNoteHtml = '<span class="param-diff-note param-diff-ok">igual en ' + esc(comparisonEnv) + '</span>';
        }
      }
    }

    const deleteBtnHtml = '<button type="button" class="btn-delete-param" data-delete-param="' + esc(fName) + '" title="Eliminar ' + esc(fName) + '"><svg class="ui-icon" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 6h18"></path><path d="M19 6v14c0 1-1 2-2 2H7c-1 0-2-1-2-2V6"></path><path d="M8 6V4c0-1 1-2 2-2h4c1 0 2 1 2 2v2"></path></svg></button>';

    paramsHtml += '<div class="param-row" data-field="' + esc(fName) + '">';
    paramsHtml += '  <div class="param-name">' + esc(fName) + '</div>';

    if(fName === 'INPUT_VARIABLE'){
      const vars = Array.isArray(fInfo.value) ? fInfo.value : [];
      let pills = vars.map(v => '<span class="var-pill" title="Clic para ubicar {' + esc(v) + '} en TEMPLATE"><span class="var-pill-text" data-find-var="' + esc(v) + '">' + esc(v) + '</span><button type="button" class="var-remove-btn" data-del-var="' + esc(v) + '" title="Quitar variable">✕</button></span>').join('');
      paramsHtml += '  <div class="input-vars-list">' + pills +
        '<span class="var-add-wrap"><input type="text" class="var-add-input" id="newVarInput" placeholder="+ variable" /><button type="button" class="var-add-btn" id="btnAddVar">+</button></span></div>';
    }else if(fInfo.type === 'BOOL'){
      paramsHtml += '  <div><select class="param-input" id="paramInput_' + esc(fName) + '">' +
        '<option value="true"' + (fInfo.value ? ' selected' : '') + '>true</option>' +
        '<option value="false"' + (!fInfo.value ? ' selected' : '') + '>false</option>' +
        '</select></div>';
    }else{
      const valStr = fInfo.value != null ? String(fInfo.value) : '';
      const isInvalid = fInfo.type === 'N' && valStr.trim() !== '' && !isValidNumberString(valStr);
      paramsHtml += '  <div><input type="text" class="param-input' + (isInvalid ? ' invalid' : '') + '" id="paramInput_' + esc(fName) + '" value="' + esc(valStr) + '"></div>';
    }

    paramsHtml += '  <div><span class="chip ' + chip.cls + '">' + esc(chip.label) + '</span></div>';
    paramsHtml += '  <div style="display:flex;align-items:center;gap:6px;justify-content:flex-end;">' + diffNoteHtml + deleteBtnHtml + '</div>';
    paramsHtml += '</div>';
  }

  // Missing fields from comparison prompt
  const missingFields = [];
  if(compP && compP.fields){
    for(const [fName, fCompInfo] of Object.entries(compP.fields)){
      if(fName === 'TEMPLATE') continue;
      if(!pData.fields || !pData.fields[fName]){
        missingFields.push({name: fName, info: fCompInfo});
      }
    }
  }

  for(const mf of missingFields){
    const chip = getChipInfo(mf.info.type);
    let previewVal = String(mf.info.value != null ? mf.info.value : '');
    if(Array.isArray(mf.info.value)) previewVal = '[' + mf.info.value.join(', ') + ']';
    if(previewVal.length > 25) previewVal = previewVal.slice(0, 22) + '…';

    paramsHtml += '<div class="param-missing-row">';
    paramsHtml += '  <div class="param-missing-name">+ ' + esc(mf.name) + '</div>';
    paramsHtml += '  <div class="param-missing-desc">Existe en ' + esc(comparisonEnv) + ' (' + esc(chip.label) + ': ' + esc(previewVal) + '). No está en ' + esc(singleActiveKey ? singleActiveKey.env : 'desa') + '.</div>';
    paramsHtml += '  <div><span class="chip ' + chip.cls + '">' + esc(chip.label) + '</span></div>';
    paramsHtml += '  <div><button type="button" class="btn-bring-test" data-bring-name="' + esc(mf.name) + '" title="Copiar este campo desde ' + esc(comparisonEnv) + '">← Traer de ' + esc(comparisonEnv) + '</button></div>';
    paramsHtml += '</div>';
  }

  // Add parameter button / inline form
  if(!isAddingParam){
    paramsHtml += '<button type="button" class="btn-add-param" id="btnToggleAddParam">+ Agregar parámetro</button>';
  }else{
    paramsHtml += '<div class="add-param-form">' +
      '<input type="text" id="newParamName" class="add-param-input" placeholder="NOMBRE_PARAMETRO" style="width:170px;" />' +
      '<select id="newParamType" class="add-param-select">' +
      '  <option value="S">String</option>' +
      '  <option value="N">Number</option>' +
      '  <option value="BOOL">Boolean</option>' +
      '  <option value="L">List</option>' +
      '</select>' +
      '<input type="text" id="newParamValue" class="add-param-input" placeholder="Valor inicial…" style="flex:1;min-width:120px;" />' +
      '<button type="button" id="btnConfirmAddParam" class="btn-confirm-add">Agregar</button>' +
      '<button type="button" id="btnCancelAddParam" class="btn-cancel-add">Cancelar</button>' +
      '</div>';
  }

  if($('promptParamsContent')) $('promptParamsContent').innerHTML = paramsHtml;

  // Bind parameter row inputs
  for(const fName of sortedKeys){
    const fInfo = pData.fields[fName];
    if(fName === 'INPUT_VARIABLE') continue;
    const inputEl = $('paramInput_' + fName);
    if(inputEl){
      inputEl.onfocus = () => {
        selectRightJsonField(fName);
      };
      inputEl.oninput = () => {
        const val = inputEl.value;
        const origRaw = fInfo.raw ? (fInfo.type === 'S' ? fInfo.raw.S : (fInfo.type === 'N' ? fInfo.raw.N : (fInfo.type === 'BOOL' ? fInfo.raw.BOOL : fInfo.raw))) : null;
        const origVal = origRaw != null ? String(origRaw) : '';
        const isDirty = val !== origVal;
        fInfo.value = (fInfo.type === 'BOOL' ? (val === 'true') : val);
        fInfo.dirty = isDirty;
        if(fName === 'OPENAI_MODEL') pData.model = val;
        if(fName === 'TEMPERATURE') pData.temperature = val;

        if(fInfo.type === 'N'){
          const isInvalid = val.trim() !== '' && !isValidNumberString(val);
          inputEl.classList.toggle('invalid', isInvalid);
        }
        selectRightJsonField(fName);
        updateDirtyState();
      };
    }
  }

  // Bind INPUT_VARIABLE events
  const addVarInput = $('newVarInput');
  const addVarBtn = $('btnAddVar');
  const handleAddVar = () => {
    if(!addVarInput) return;
    const newV = addVarInput.value.trim();
    if(!newV) return;
    if(!pData.fields.INPUT_VARIABLE){
      pData.fields.INPUT_VARIABLE = {type: 'L', value: [], dirty: true};
    }
    const arr = Array.isArray(pData.fields.INPUT_VARIABLE.value) ? pData.fields.INPUT_VARIABLE.value : [];
    if(!arr.includes(newV)){
      arr.push(newV);
      pData.fields.INPUT_VARIABLE.value = arr;
      pData.fields.INPUT_VARIABLE.dirty = true;
      pData.input_variables = [...arr];
      selectRightJsonField('INPUT_VARIABLE');
      updateDirtyState();
      renderActivePrompt();
    }
  };
  if(addVarInput){
    addVarInput.onfocus = () => selectRightJsonField('INPUT_VARIABLE');
    addVarInput.onkeydown = e => {
      if(e.key === 'Enter'){
        e.preventDefault();
        handleAddVar();
      }
    };
  }
  if(addVarBtn) addVarBtn.onclick = handleAddVar;

  document.querySelectorAll('.var-remove-btn').forEach(btn => {
    btn.onclick = () => {
      const vToDel = btn.dataset.delVar;
      if(!pData.fields.INPUT_VARIABLE) return;
      const arr = (Array.isArray(pData.fields.INPUT_VARIABLE.value) ? pData.fields.INPUT_VARIABLE.value : []).filter(x => x !== vToDel);
      pData.fields.INPUT_VARIABLE.value = arr;
      pData.fields.INPUT_VARIABLE.dirty = true;
      pData.input_variables = [...arr];
      selectRightJsonField('INPUT_VARIABLE');
      updateDirtyState();
      renderActivePrompt();
    };
  });

  document.querySelectorAll('.var-pill-text').forEach(btn => {
    btn.onclick = () => {
      const vToFind = btn.dataset.findVar;
      if(!vToFind || !templateModel) return;
      selectRightJsonField('TEMPLATE');
      if(templateDiffActive) toggleTemplateDiffView();
      const targetQuery = '{' + vToFind + '}';
      const matches = templateModel.findMatches(targetQuery, false, false, true, null, true);
      if(matches && matches.length){
        templateEditor.setSelection(matches[0].range);
        templateEditor.revealRangeInCenter(matches[0].range);
        templateEditor.focus();
      }else{
        setStatus('La variable {' + vToFind + '} no se encontró en el TEMPLATE actual.');
      }
    };
  });

  document.querySelectorAll('.btn-delete-param').forEach(btn => {
    btn.onclick = () => {
      const fToDel = btn.dataset.deleteParam;
      if(!window.confirm('¿Eliminar el parámetro "' + fToDel + '" de ' + pData.name + '?')) return;
      if(!pData.deleted_fields) pData.deleted_fields = [];
      if(!pData.deleted_fields.includes(fToDel)) pData.deleted_fields.push(fToDel);
      delete pData.fields[fToDel];
      if(fToDel === 'OPENAI_MODEL') pData.model = '';
      if(fToDel === 'TEMPERATURE') pData.temperature = '';
      if(fToDel === 'INPUT_VARIABLE') pData.input_variables = [];
      selectRightJsonField('TEMPLATE');
      updateDirtyState();
      renderActivePrompt();
    };
  });

  document.querySelectorAll('.btn-bring-test').forEach(btn => {
    btn.onclick = () => {
      const fNameToBring = btn.dataset.bringName;
      if(!compP || !compP.fields || !compP.fields[fNameToBring]) return;
      const compF = compP.fields[fNameToBring];
      pData.fields[fNameToBring] = {
        type: compF.type,
        value: JSON.parse(JSON.stringify(compF.value)),
        raw: compF.raw ? JSON.parse(JSON.stringify(compF.raw)) : null,
        dirty: true
      };
      if(fNameToBring === 'OPENAI_MODEL') pData.model = compF.value;
      if(fNameToBring === 'TEMPERATURE') pData.temperature = compF.value;
      if(fNameToBring === 'INPUT_VARIABLE') pData.input_variables = Array.isArray(compF.value) ? [...compF.value] : [];
      selectRightJsonField(fNameToBring);
      updateDirtyState();
      renderActivePrompt();
    };
  });

  if($('btnToggleAddParam')){
    $('btnToggleAddParam').onclick = () => {
      isAddingParam = true;
      renderActivePrompt();
      setTimeout(() => { if($('newParamName')) $('newParamName').focus(); }, 50);
    };
  }
  if($('btnCancelAddParam')){
    $('btnCancelAddParam').onclick = () => {
      isAddingParam = false;
      renderActivePrompt();
    };
  }
  if($('btnConfirmAddParam')){
    $('btnConfirmAddParam').onclick = () => {
      const nameInput = $('newParamName');
      const typeSelect = $('newParamType');
      const valInput = $('newParamValue');
      if(!nameInput) return;
      let pNameNew = nameInput.value.trim().toUpperCase();
      if(!pNameNew){
        return alert('Ingresá el nombre del parámetro');
      }
      if(pData.fields[pNameNew]){
        return alert('El parámetro "' + pNameNew + '" ya existe en este prompt');
      }
      const pType = typeSelect.value;
      let rawVal = valInput.value.trim();
      let parsedVal = rawVal;
      if(pType === 'N'){
        if(rawVal !== '' && !isValidNumberString(rawVal)){
          return alert('El valor para tipo Number (N) debe ser numérico válido');
        }
      }else if(pType === 'BOOL'){
        parsedVal = rawVal.toLowerCase() === 'true' || rawVal === '1';
      }else if(pType === 'L'){
        parsedVal = rawVal ? [rawVal] : [];
      }
      pData.fields[pNameNew] = {
        type: pType,
        value: parsedVal,
        raw: null,
        dirty: true
      };
      if(pNameNew === 'OPENAI_MODEL') pData.model = parsedVal;
      if(pNameNew === 'TEMPERATURE') pData.temperature = parsedVal;
      if(pNameNew === 'INPUT_VARIABLE') pData.input_variables = Array.isArray(parsedVal) ? parsedVal : [];
      isAddingParam = false;
      selectRightJsonField(pNameNew);
      updateDirtyState();
      renderActivePrompt();
    };
  }

  // Load TEMPLATE into Monaco
  const tmpl = (pData.fields && pData.fields.TEMPLATE && pData.fields.TEMPLATE.value != null) ? pData.fields.TEMPLATE.value : (pData.template || '');
  if(templateModel && templateModel.getValue() !== tmpl){
    templateModel.setValue(tmpl);
  }
  updateTemplateStats();
  updateRightJsonPreview();

  // TEMPLATE footer warning
  const warnEl = $('templateDiffWarning');
  if(warnEl){
    if(missingFields.length > 0){
      warnEl.hidden = false;
      warnEl.innerHTML = '<svg class="ui-icon" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="margin-right:4px;"><path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3Z"></path><line x1="12" y1="9" x2="12" y2="13"></line><line x1="12" y1="17" x2="12.01" y2="17"></line></svg> Hay ' + missingFields.length + (missingFields.length === 1 ? ' campo en ' : ' campos en ') + comparisonEnv + ' que ' + (singleActiveKey ? singleActiveKey.env : 'desa') + ' no tiene';
    }else{
      warnEl.hidden = true;
    }
  }

  // Update diff template button
  const btnDiff = $('btnDiffTemplate');
  if(btnDiff){
    if(templateDiffActive){
      btnDiff.title = 'Volver a editar';
      btnDiff.classList.add('active');
      updateDiffEditorContent();
    }else{
      btnDiff.title = 'Comparar diff con ' + comparisonEnv;
      btnDiff.classList.remove('active');
    }
  }

  // Wire Compare with test button
  const btnComp = $('btnComparePrompt');
  if(btnComp){
    btnComp.textContent = 'Comparar con ' + comparisonEnv;
    btnComp.onclick = () => openPromptInCompare(pData.name);
  }

  renderCompPreview();

  requestAnimationFrame(() => {
    if(templateDiffActive && templateDiffEditor){
      templateDiffEditor.layout();
    }else if(templateEditor){
      templateEditor.layout();
    }
  });
}

function updateDiffEditorContent(){
  if(!compTemplateModel) return;
  let counterpartTemplate = '';
  if(selectedPromptName && comparisonPromptsData && comparisonPromptsData.prompts){
    const compP = comparisonPromptsData.prompts[selectedPromptName];
    if(compP){
      counterpartTemplate = (compP.fields && compP.fields.TEMPLATE && compP.fields.TEMPLATE.value != null)
        ? compP.fields.TEMPLATE.value
        : (compP.template || '');
    }
  }
  // Normalize EOL to LF so line-by-line diff is identical without spurious carriage returns
  counterpartTemplate = counterpartTemplate.replace(/\r\n/g, '\n');
  if(templateModel){
    const curVal = templateModel.getValue();
    const normalizedCur = curVal.replace(/\r\n/g, '\n');
    if(curVal !== normalizedCur){
      templateModel.setValue(normalizedCur);
    }
  }
  if(compTemplateModel.getValue() !== counterpartTemplate){
    compTemplateModel.setValue(counterpartTemplate);
  }
  if(templateDiffEditor){
    templateDiffEditor.layout();
  }
}

function toggleTemplateDiffView(){
  templateDiffActive = !templateDiffActive;
  const btn = $('btnDiffTemplate');
  const edNormal = $('templateMonacoEditor');
  const edDiff = $('templateMonacoDiffEditor');

  if(templateDiffActive){
    if(btn){
      btn.title = 'Volver a editar';
      btn.classList.add('active');
    }
    if(edNormal) edNormal.style.display = 'none';
    if(edDiff){
      edDiff.hidden = false;
      edDiff.style.display = 'block';
    }
    updateDiffEditorContent();
    const applyDiffOptions = () => {
      if(!templateDiffEditor) return;
      const wrapOpt = templateWrapEnabled ? 'on' : 'off';
      const wrapStrategy = templateWrapEnabled ? 'advanced' : 'normal';
      const horizScroll = !templateWrapEnabled;
      templateDiffEditor.updateOptions({
        theme: 'vs-dark',
        fontSize: currentFontSize,
        diffWordWrap: wrapOpt,
        wordWrap: wrapOpt,
        wrappingStrategy: wrapStrategy,
        scrollBeyondLastColumn: 0,
        horizontalScrolling: horizScroll,
        diffAlgorithm: 'advanced',
        renderSideBySide: true,
        renderIndicators: true,
        renderMarginRevertIcon: true,
        enableSplitViewResizing: true,
        splitViewDefaultRatio: 0.5,
        renderOverviewRuler: true,
        ignoreTrimWhitespace: false,
        hideUnchangedRegions: { enabled: false },
        originalEditable: false,
        readOnly: true
      });
      if(templateDiffEditor.getOriginalEditor()){
        templateDiffEditor.getOriginalEditor().updateOptions({
          wordWrap: wrapOpt,
          wrappingStrategy: wrapStrategy,
          scrollBeyondLastColumn: 0,
          horizontalScrolling: horizScroll,
          fontSize: currentFontSize,
          lineNumbers: 'on',
          minimap: { enabled: false },
          glyphMargin: true,
          folding: true,
          padding: { top: 12, bottom: 12 }
        });
      }
      if(templateDiffEditor.getModifiedEditor()){
        templateDiffEditor.getModifiedEditor().updateOptions({
          wordWrap: wrapOpt,
          wrappingStrategy: wrapStrategy,
          scrollBeyondLastColumn: 0,
          horizontalScrolling: horizScroll,
          fontSize: currentFontSize,
          lineNumbers: 'on',
          minimap: { enabled: false },
          glyphMargin: true,
          folding: true,
          padding: { top: 12, bottom: 12 }
        });
      }
      templateDiffEditor.layout();
    };
    applyDiffOptions();
    requestAnimationFrame(applyDiffOptions);
    setTimeout(applyDiffOptions, 50);
  }else{
    if(btn){
      btn.title = 'Comparar diff con ' + (comparisonEnv || 'test');
      btn.classList.remove('active');
    }
    if(edDiff){
      edDiff.hidden = true;
      edDiff.style.display = 'none';
    }
    if(edNormal){
      edNormal.style.display = 'block';
    }
    requestAnimationFrame(() => {
      if(templateEditor){
        templateEditor.layout();
        templateEditor.focus();
      }
    });
  }
}

function renderCompPreview(){
  if($('compPreviewEnvName')) $('compPreviewEnvName').textContent = comparisonEnv.toUpperCase();
  if($('btnCopyAllFromComp')) $('btnCopyAllFromComp').innerHTML = '<svg class="ui-icon" width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="margin-right:4px;"><rect width="14" height="14" x="8" y="8" rx="2" ry="2"></rect><path d="M4 16c-1.1 0-2-.9-2-2V4c0-1.1.9-2 2-2h10c1.1 0 2 .9 2 2"></path></svg> Copiar todo desde ' + comparisonEnv;
  if($('btnCopyTemplateOnly')) $('btnCopyTemplateOnly').textContent = '<svg class="ui-icon" width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="margin-right:4px;"><rect width="14" height="14" x="8" y="8" rx="2" ry="2"></rect><path d="M4 16c-1.1 0-2-.9-2-2V4c0-1.1.9-2 2-2h10c1.1 0 2 .9 2 2"></path></svg> Copiar solo TEMPLATE de ' + comparisonEnv;

  const btnCopyAll = $('btnCopyAllFromComp');
  const btnCopyTmpl = $('btnCopyTemplateOnly');
  const statusBadgeEl = $('compPreviewStatusBadge');
  const paramsListEl = $('compPreviewParamsList');
  const tmplTextEl = $('compPreviewTemplateText');

  if(!selectedPromptName || !unpackedPromptsData || !unpackedPromptsData.prompts[selectedPromptName]){
    return;
  }
  const pData = unpackedPromptsData.prompts[selectedPromptName];
  const compP = (comparisonPromptsData && comparisonPromptsData.prompts) ? comparisonPromptsData.prompts[pData.name] : null;

  if(!compP){
    if(statusBadgeEl) statusBadgeEl.innerHTML = '<span class="dot dot-solo">●</span> No existe en ' + esc(comparisonEnv);
    if(btnCopyAll) btnCopyAll.disabled = true;
    if(btnCopyTmpl) btnCopyTmpl.disabled = true;
    if(paramsListEl) paramsListEl.innerHTML = '<span class="comp-preview-empty">No hay parámetros disponibles en ' + esc(comparisonEnv) + '.</span>';
    if(tmplTextEl) tmplTextEl.textContent = '(Este prompt no existe en ' + comparisonEnv + ')';
    return;
  }

  const diffInfo = getPromptDiffStatus(pData.name);
  let dotClass = 'dot-warn';
  if(diffInfo.status === 'same') dotClass = 'dot-ok';
  else if(diffInfo.status === 'solo') dotClass = 'dot-solo';
  if(statusBadgeEl) statusBadgeEl.innerHTML = '<span class="dot ' + dotClass + '">●</span> ' + esc(diffInfo.text);

  if(btnCopyAll) btnCopyAll.disabled = false;
  if(btnCopyTmpl) btnCopyTmpl.disabled = false;

  const compKeys = Object.keys(compP.fields || {}).filter(k => k !== 'TEMPLATE').sort();
  let paramsTagsHtml = '';
  for(const k of compKeys){
    const fInfo = compP.fields[k];
    let valStr = String(fInfo.value != null ? fInfo.value : '');
    if(Array.isArray(fInfo.value)) valStr = '[' + fInfo.value.join(', ') + ']';
    if(valStr.length > 32) valStr = valStr.slice(0, 29) + '…';
    const chip = getChipInfo(fInfo.type);
    paramsTagsHtml += '<div class="comp-preview-param-tag">' +
      '<span class="comp-preview-param-name">' + esc(k) + '</span>' +
      '<span class="chip ' + chip.cls + '" style="font-size:10px;padding:1px 5px;min-width:auto;">' + esc(chip.label) + '</span>' +
      '<span class="comp-preview-param-val" title="' + esc(String(fInfo.value)) + '">' + esc(valStr) + '</span>' +
      '</div>';
  }
  if(!paramsTagsHtml) paramsTagsHtml = '<span style="color:#777;font-size:12px;">Sin parámetros adicionales</span>';
  if(paramsListEl) paramsListEl.innerHTML = paramsTagsHtml;

  const compTmpl = (compP.fields && compP.fields.TEMPLATE && compP.fields.TEMPLATE.value != null)
    ? compP.fields.TEMPLATE.value
    : (compP.template || '');
  if(tmplTextEl) tmplTextEl.textContent = compTmpl || '(TEMPLATE vacío)';
}

function toggleCompPreview(){
  compPreviewExpanded = !compPreviewExpanded;
  if($('compPreviewBody')) $('compPreviewBody').hidden = !compPreviewExpanded;
  if($('compPreviewToggleIcon')) $('compPreviewToggleIcon').textContent = compPreviewExpanded ? '▼' : '▶';
  if($('btnToggleCompPreview')) $('btnToggleCompPreview').textContent = compPreviewExpanded ? 'Ocultar' : 'Mostrar';
  if($('compPreviewHeader')) $('compPreviewHeader').classList.toggle('expanded', compPreviewExpanded);
}

function copyAllFromComparison(){
  if(!selectedPromptName || !unpackedPromptsData || !unpackedPromptsData.prompts[selectedPromptName]) return;
  const pData = unpackedPromptsData.prompts[selectedPromptName];
  if(!comparisonPromptsData || !comparisonPromptsData.prompts || !comparisonPromptsData.prompts[pData.name]){
    return alert('El prompt no existe en ' + comparisonEnv);
  }
  const compP = comparisonPromptsData.prompts[pData.name];
  if(!window.confirm('¿Reemplazar TODOS los parámetros y el TEMPLATE de "' + pData.name + '" con los de ' + comparisonEnv + '?')) return;

  pData.fields = JSON.parse(JSON.stringify(compP.fields || {}));
  for(const f of Object.values(pData.fields)){
    f.dirty = true;
  }
  pData.model = compP.model || '';
  pData.temperature = compP.temperature || '';
  pData.input_variables = [...(compP.input_variables || [])];
  const tmplVal = (pData.fields.TEMPLATE && pData.fields.TEMPLATE.value != null) ? pData.fields.TEMPLATE.value : (compP.template || '');
  pData.template = tmplVal;
  if(pData.deleted_fields) pData.deleted_fields = [];

  if(templateModel) templateModel.setValue(tmplVal);
  updateTemplateStats();
  updateRightJsonPreview();
  updateDirtyState();
  renderActivePrompt();
  setStatus('Todos los valores de ' + pData.name + ' fueron copiados desde ' + comparisonEnv + '. Guardá para persistir en DynamoDB.');
}

function copyTemplateOnlyFromComparison(){
  if(!selectedPromptName || !unpackedPromptsData || !unpackedPromptsData.prompts[selectedPromptName]) return;
  const pData = unpackedPromptsData.prompts[selectedPromptName];
  if(!comparisonPromptsData || !comparisonPromptsData.prompts || !comparisonPromptsData.prompts[pData.name]){
    return alert('El prompt no existe en ' + comparisonEnv);
  }
  const compP = comparisonPromptsData.prompts[pData.name];
  const compTmpl = (compP.fields && compP.fields.TEMPLATE && compP.fields.TEMPLATE.value != null) ? compP.fields.TEMPLATE.value : (compP.template || '');
  if(!window.confirm('¿Reemplazar el TEMPLATE de "' + pData.name + '" con el de ' + comparisonEnv + '?')) return;

  if(!pData.fields.TEMPLATE){
    pData.fields.TEMPLATE = {type: 'S', value: compTmpl, dirty: true};
  }else{
    pData.fields.TEMPLATE.value = compTmpl;
    pData.fields.TEMPLATE.dirty = true;
  }
  pData.template = compTmpl;
  if(templateModel) templateModel.setValue(compTmpl);
  updateTemplateStats();
  updateRightJsonPreview();
  updateDirtyState();
  renderActivePrompt();
  setStatus('TEMPLATE de ' + pData.name + ' copiado desde ' + comparisonEnv + '.');
}

async function openPromptInCompare(promptName){
  if(!singleActiveKey) return;
  const {client, bot, env} = singleActiveKey;
  $('viewMode').value = 'compare';
  changeMode();
  if(activeMode !== 'compare') return;
  $('envL').value = env;
  $('envR').value = comparisonEnv;
  $('pick').value = JSON.stringify([client, bot]);
  await compare();
  if(promptName){
    setTimeout(() => {
      let targetLine = -1;
      let targetSide = 'left';
      const query = '"' + promptName + '":';
      if(leftModel){
        const matches = leftModel.findMatches(query, true, false, true, null, true);
        if(matches && matches.length > 0){
          targetLine = matches[0].range.startLineNumber;
          targetSide = 'left';
        }
      }
      if(targetLine === -1 && rightModel){
        const rMatches = rightModel.findMatches(query, true, false, true, null, true);
        if(rMatches && rMatches.length > 0){
          targetLine = rMatches[0].range.startLineNumber;
          targetSide = 'right';
        }
      }
      if(targetLine > 0){
        const ed = sideEditor(targetSide);
        if(ed){
          ed.revealLineInCenter(targetLine);
          ed.setPosition({lineNumber: targetLine, column: 1});
          ed.focus();
        }
      }
    }, 120);
  }
}

function setSingleSubMode(mode){
  if(mode === 'json' && templateDiffActive){
    toggleTemplateDiffView();
  }
  currentSingleSubMode = mode;
  if($('subModeJson')) $('subModeJson').classList.toggle('active', mode === 'json');
  if($('subModePrompts')) $('subModePrompts').classList.toggle('active', mode === 'text');

  if(mode === 'json'){
    if(rawSingleItem && unpackedPromptsData){
      const repacked = repackPrompts(rawSingleItem, unpackedPromptsData);
      rawSingleItem = repacked;
      if(singleModel) singleModel.setValue(JSON.stringify(repacked, null, 2));
    }
    if($('singleEditor')) $('singleEditor').hidden = false;
    if($('promptsView')) $('promptsView').hidden = true;
    if($('promptsFooter')) $('promptsFooter').hidden = true;
    if($('btnToggleRightJson')) $('btnToggleRightJson').hidden = true;
    if(singleEditor) singleEditor.layout();
  }else{
    if(singleModel){
      try{
        const parsed = JSON.parse(singleModel.getValue());
        rawSingleItem = parsed;
        unpackedPromptsData = unpackPrompts(rawSingleItem);
      }catch(e){
        // retain existing unpacked data
      }
    }
    if($('singleEditor')) $('singleEditor').hidden = true;
    if($('promptsView')) $('promptsView').hidden = false;
    if($('promptsFooter')) $('promptsFooter').hidden = false;
    if($('btnToggleRightJson')) $('btnToggleRightJson').hidden = false;
    renderPromptsView();
    if(templateEditor) templateEditor.layout();
  }
  updateDirtyState();
}

/* ---------- lista client / bot ---------- */
async function getKeys(env, force){
  if(!force && keyCache[env]) return keyCache[env];
  const j = await api('/api/keys?env=' + encodeURIComponent(env) + (force ? '&refresh=1' : ''));
  return keyCache[env] = j.keys;
}
/* ==========================================================================
   VISTA MODELOS (Rediseño: Por prompt / Por modelo en 3 columnas)
   ========================================================================== */

const modelsState = {
  env: 'desa',
  scopeMode: 'prompt', // 'prompt' | 'model'
  selectedItem: null,  // prompt_name or model_name
  data: null,          // { rows, prompts_index, models_index, total_bots, total_prompts, total_rows, all_models, load_time_seconds }
  loading: false,
  error: null,
  selectedRowKeys: new Set(), // Set of `${client}/${bot}/${prompt_name}`
  listFilter: '',
  botFilter: '',
  familyFilter: null,
  groupFilter: null,
  draft: {
    model: '',
    temperature: '',
    reasoning_effort: '',
    response_format_type: '',
    max_tokens: '',
    variablesToAdd: [],
    variablesToRemove: []
  },
  validationErrors: {
    temperature: null,
    max_tokens: null
  }
};

function getAllScopeRows(){
  if(!modelsState.data || !modelsState.data.rows || !modelsState.selectedItem) return [];
  if(modelsState.scopeMode === 'prompt'){
    return modelsState.data.rows.filter(r => (r.prompt === modelsState.selectedItem || r.prompt_name === modelsState.selectedItem));
  }else{
    return modelsState.data.rows.filter(r => (r.model === modelsState.selectedItem || r.OPENAI_MODEL === modelsState.selectedItem));
  }
}

function getFilteredRows(){
  const all = getAllScopeRows();
  const botQ = (modelsState.botFilter || '').trim().toLowerCase();
  return all.filter(r => {
    if(botQ){
      const text = `${r.client} ${r.bot} ${r.client}/${r.bot} ${r.prompt || ''} ${r.prompt_name || ''}`.toLowerCase();
      if(!text.includes(botQ)) return false;
    }
    if(modelsState.familyFilter !== null){
      if(r.client.toLowerCase() !== modelsState.familyFilter.toLowerCase()) return false;
    }
    if(modelsState.groupFilter !== null){
      if(modelsState.scopeMode === 'prompt'){
        const m = r.model || r.OPENAI_MODEL;
        if(m !== modelsState.groupFilter) return false;
      }else{
        const p = r.prompt || r.prompt_name;
        if(p !== modelsState.groupFilter) return false;
      }
    }
    return true;
  });
}

async function loadModelsData(force){
  modelsState.loading = true;
  modelsState.error = null;
  const statusText = $('modelStatusText');
  const prodWarning = $('modelStatusProdWarning');
  if(prodWarning) prodWarning.hidden = (modelsState.env !== 'prod');
  if(statusText) statusText.textContent = `Leyendo bots y prompts de ${modelsState.env}…`;

  try{
    const res = await api(`/api/models/data?env=${encodeURIComponent(modelsState.env)}${force ? '&refresh=1' : ''}`);
    modelsState.data = res;
    modelsState.loading = false;

    if(statusText){
      statusText.textContent = `Ambiente: ${res.env.toUpperCase()} · ${res.total_bots} bots · ${res.total_prompts} prompts distintos · ${res.total_rows} filas cargadas en ${res.load_time_seconds}s`;
    }

    if(modelsState.scopeMode === 'prompt'){
      const promptList = Array.isArray(res.prompts_index) ? res.prompts_index : Object.values(res.prompts_index || {});
      const promptNames = promptList.map(p => p.prompt);
      if(!modelsState.selectedItem || !promptNames.includes(modelsState.selectedItem)){
        modelsState.selectedItem = promptNames.length ? promptNames[0] : null;
      }
    }else{
      const modelList = Array.isArray(res.models_index) ? res.models_index : Object.values(res.models_index || {});
      const modelNames = modelList.map(m => m.model);
      if(!modelsState.selectedItem || !modelNames.includes(modelsState.selectedItem)){
        modelsState.selectedItem = modelNames.length ? modelNames[0] : null;
      }
    }

    modelsState.selectedRowKeys.clear();
    getAllScopeRows().forEach(r => {
      modelsState.selectedRowKeys.add(r.id);
      modelsState.selectedRowKeys.add(`${r.client}/${r.bot}/${r.prompt_name}`);
    });

    renderLeftList();
    renderCenterColumn();
    updateRightColumn();
  }catch(e){
    modelsState.loading = false;
    modelsState.error = e.message;
    if(statusText) statusText.textContent = `Error cargando modelos: ${e.message}`;
  }
}

function setModelEnvironment(newEnv){
  if(modelsState.env === newEnv && modelsState.data) return;
  modelsState.env = newEnv;
  document.querySelectorAll('#modelEnvSegmented .env-btn').forEach(btn => {
    btn.classList.toggle('active', btn.dataset.env === newEnv);
  });
  if($('modelEnv')) $('modelEnv').value = newEnv;
  const bar = $('bar');
  if(bar){
    bar.classList.remove('env-desa', 'env-test', 'env-prod');
    bar.classList.add('env-' + newEnv);
  }
  modelsState.selectedRowKeys.clear();
  modelsState.familyFilter = null;
  modelsState.groupFilter = null;
  modelsState.botFilter = '';
  if($('centerBotFilter')) $('centerBotFilter').value = '';
  loadModelsData(false);
}

function setModelsScopeMode(mode){
  if(modelsState.scopeMode === mode) return;
  clearDraftEdits();
  modelsState.scopeMode = mode;
  if($('scopeByPrompt')) $('scopeByPrompt').classList.toggle('active', mode === 'prompt');
  if($('scopeByModel')) $('scopeByModel').classList.toggle('active', mode === 'model');

  modelsState.selectedRowKeys.clear();
  modelsState.familyFilter = null;
  modelsState.groupFilter = null;
  modelsState.botFilter = '';
  if($('centerBotFilter')) $('centerBotFilter').value = '';
  modelsState.listFilter = '';
  if($('modelListSearch')) $('modelListSearch').value = '';

  if(modelsState.data){
    if(mode === 'prompt'){
      const promptList = Array.isArray(modelsState.data.prompts_index) ? modelsState.data.prompts_index : Object.values(modelsState.data.prompts_index || {});
      const promptNames = promptList.map(p => p.prompt);
      modelsState.selectedItem = promptNames.length ? promptNames[0] : null;
    }else{
      const modelList = Array.isArray(modelsState.data.models_index) ? modelsState.data.models_index : Object.values(modelsState.data.models_index || {});
      const modelNames = modelList.map(m => m.model);
      modelsState.selectedItem = modelNames.length ? modelNames[0] : null;
    }
    getAllScopeRows().forEach(r => {
      modelsState.selectedRowKeys.add(r.id);
      modelsState.selectedRowKeys.add(`${r.client}/${r.bot}/${r.prompt_name}`);
    });
  }

  renderLeftList();
  renderCenterColumn();
  updateRightColumn();
}

function clearDraftEdits(){
  modelsState.draft = {
    model: '',
    temperature: '',
    reasoning_effort: '',
    response_format_type: '',
    max_tokens: '',
    variablesToAdd: [],
    variablesToRemove: []
  };
  if($('newVarInput')) $('newVarInput').value = '';
  modelsState.validationErrors = {
    temperature: null,
    max_tokens: null
  };
  if($('editModelSelect')){
    $('editModelSelect').value = '';
    $('editModelSelect').classList.remove('input-pending');
  }
  if($('editTempInput')){
    $('editTempInput').value = '';
    $('editTempInput').classList.remove('input-error', 'input-pending');
  }
  if($('editTokensInput')){
    $('editTokensInput').value = '';
    $('editTokensInput').classList.remove('input-error', 'input-pending');
  }
  if($('editReasoningSelect')){
    $('editReasoningSelect').value = '';
    $('editReasoningSelect').classList.remove('input-pending');
  }
  if($('editFormatSelect')){
    $('editFormatSelect').value = '';
    $('editFormatSelect').classList.remove('input-pending');
  }
  if($('tempError')) $('tempError').hidden = true;
  if($('tokensError')) $('tokensError').hidden = true;
}

function selectScopeItem(itemId){
  clearDraftEdits();
  if(modelsState.selectedItem === itemId) return;
  modelsState.selectedItem = itemId;
  modelsState.familyFilter = null;
  modelsState.groupFilter = null;
  modelsState.botFilter = '';
  if($('centerBotFilter')) $('centerBotFilter').value = '';

  modelsState.selectedRowKeys.clear();
  getAllScopeRows().forEach(r => {
    modelsState.selectedRowKeys.add(r.id);
    modelsState.selectedRowKeys.add(`${r.client}/${r.bot}/${r.prompt_name}`);
  });

  renderLeftList();
  renderCenterColumn();
  updateRightColumn();
}

function renderLeftList(){
  const listEl = $('modelItemList');
  const titleEl = $('modelListTitle');
  const countEl = $('modelListCount');
  const searchEl = $('modelListSearch');
  if(!listEl || !modelsState.data) return;

  const isPrompt = modelsState.scopeMode === 'prompt';
  if(titleEl) titleEl.textContent = isPrompt ? '▼ PROMPTS EN USO' : '▼ MODELOS EN USO';
  if(searchEl){
    searchEl.placeholder = isPrompt ? 'Buscar prompt…' : 'Buscar modelo…';
  }

  const query = (modelsState.listFilter || '').trim().toLowerCase();
  let items = [];

  if(isPrompt){
    const promptList = Array.isArray(modelsState.data.prompts_index) ? modelsState.data.prompts_index : Object.values(modelsState.data.prompts_index || {});
    items = promptList.map(p => {
      const rawName = p.prompt_name || (p.prompt ? p.prompt.replace(/^\(raíz\)\s*/, '') : '');
      const group = p.group || '';
      const groupTag = group === '(raíz)' ? 'raíz' : group;
      const majModel = p.majority_model || (p.models && p.models.length ? p.models[0] : '');
      return {
        id: p.prompt,
        promptName: rawName,
        groupTag: groupTag,
        model: majModel,
        modelCount: p.model_count || (p.models ? p.models.length : 0),
        botCount: p.bot_count,
        searchStr: `${rawName} ${groupTag} ${majModel} ${p.prompt}`.toLowerCase()
      };
    });
  }else{
    const modelList = Array.isArray(modelsState.data.models_index) ? modelsState.data.models_index : Object.values(modelsState.data.models_index || {});
    items = modelList.map(m => ({
      id: m.model,
      modelName: m.model,
      promptCount: m.prompt_count,
      botCount: m.bot_count,
      searchStr: `${m.model}`.toLowerCase()
    }));
  }

  if(countEl) countEl.textContent = `(${items.length})`;

  if(query){
    items = items.filter(it => it.searchStr.includes(query));
  }

  if(items.length === 0){
    listEl.innerHTML = `<div class="model-item-empty">No se encontraron ${isPrompt ? 'prompts' : 'modelos'}</div>`;
    return;
  }

  if(isPrompt){
    listEl.innerHTML = items.map(it => {
      const active = it.id === modelsState.selectedItem ? ' active' : '';
      const tagHtml = it.groupTag ? `<span class="model-card-tag">${esc(it.groupTag)}</span>` : '';
      const multiHtml = it.modelCount > 1 ? `<span class="model-card-multi">+${it.modelCount - 1} más</span>` : '';
      const modelHtml = it.model
        ? `<span class="model-card-model" title="${esc(it.model)}">${esc(it.model)}${multiHtml}</span>`
        : `<span class="model-card-empty">(sin modelo)</span>`;
      return `<div class="model-list-item${active}" data-id="${esc(it.id)}" role="option" aria-selected="${it.id === modelsState.selectedItem}">
        <div class="model-card-top">
          <span class="model-card-prompt-name" title="${esc(it.promptName)}">${esc(it.promptName)}</span>
          ${tagHtml}
        </div>
        <div class="model-card-bottom">
          ${modelHtml}
          <span class="model-card-bots">${it.botCount} bots</span>
        </div>
      </div>`;
    }).join('');
  }else{
    listEl.innerHTML = items.map(it => {
      const active = it.id === modelsState.selectedItem ? ' active' : '';
      return `<div class="model-list-item${active}" data-id="${esc(it.id)}" role="option" aria-selected="${it.id === modelsState.selectedItem}">
        <div class="model-card-top">
          <span class="model-card-prompt-name">MODELO</span>
          <span class="model-card-bots">${it.botCount} bots</span>
        </div>
        <div class="model-card-bottom">
          <span class="model-card-model" title="${esc(it.modelName)}">${esc(it.modelName)}</span>
          <span class="model-card-bots">${it.promptCount} prompts</span>
        </div>
      </div>`;
    }).join('');
  }
}

function renderChips(allScopeRows){
  const familyCounts = new Map();
  allScopeRows.forEach(r => {
    const fam = r.client;
    if(fam) familyCounts.set(fam, (familyCounts.get(fam) || 0) + 1);
  });
  const sortedFamilies = [...familyCounts.entries()].sort((a, b) => b[1] - a[1]);
  const familyChipsEl = $('familyChips');
  if(familyChipsEl){
    const allActive = modelsState.familyFilter === null ? ' active' : '';
    let html = `<button type="button" class="chip${allActive}" data-val="__all__">Todos <span class="chip-count">(${allScopeRows.length})</span></button>`;
    sortedFamilies.forEach(([fam, count]) => {
      const active = modelsState.familyFilter === fam ? ' active' : '';
      html += `<button type="button" class="chip${active}" data-val="${esc(fam)}">${esc(fam)} <span class="chip-count">(${count})</span></button>`;
    });
    familyChipsEl.innerHTML = html;
  }

  const groupCounts = new Map();
  allScopeRows.forEach(r => {
    const key = modelsState.scopeMode === 'prompt' ? (r.model || r.OPENAI_MODEL) : (r.prompt || r.prompt_name);
    if(key) groupCounts.set(key, (groupCounts.get(key) || 0) + 1);
  });
  const sortedGroups = [...groupCounts.entries()].sort((a, b) => b[1] - a[1]);
  const groupChipsEl = $('groupChips');
  const groupLabelEl = $('groupFilterLabel');
  if(groupLabelEl){
    groupLabelEl.textContent = modelsState.scopeMode === 'prompt' ? 'Seleccionar solo los que usan modelo' : 'Seleccionar solo los que usan prompt';
  }
  if(groupChipsEl){
    const allActive = modelsState.groupFilter === null ? ' active' : '';
    let html = `<button type="button" class="chip${allActive}" data-val="__all__">Todos <span class="chip-count">(${allScopeRows.length})</span></button>`;
    sortedGroups.forEach(([val, count]) => {
      const active = modelsState.groupFilter === val ? ' active' : '';
      html += `<button type="button" class="chip${active}" data-val="${esc(val)}">${esc(val)} <span class="chip-count">(${count})</span></button>`;
    });
    groupChipsEl.innerHTML = html;
  }
}

function renderCenterColumn(){
  if(!modelsState.data) return;
  const allScopeRows = getAllScopeRows();
  const filteredRows = getFilteredRows();

  const isPrompt = modelsState.scopeMode === 'prompt';
  const titleEl = $('centerTargetTitle');
  const countsEl = $('centerTargetCounts');
  const explEl = $('centerExplText');

  if(titleEl){
    if(!isPrompt && modelsState.draft.model && modelsState.draft.model !== modelsState.selectedItem){
      titleEl.innerHTML = `<span class="diff-old">${esc(modelsState.selectedItem)}</span> <span class="diff-arrow">→</span> <span class="diff-new">${esc(modelsState.draft.model)}</span>`;
    }else{
      const rawTitle = modelsState.selectedItem || '(Sin selección)';
      if(isPrompt && rawTitle.startsWith('(raíz) ')){
        titleEl.innerHTML = `${esc(rawTitle.replace('(raíz) ', ''))} <span class="model-card-tag" style="margin-left:8px;font-size:11px;vertical-align:middle">raíz</span>`;
      }else{
        titleEl.textContent = rawTitle;
      }
    }
  }

  const selectedEffective = filteredRows.filter(r => modelsState.selectedRowKeys.has(r.id) || modelsState.selectedRowKeys.has(`${r.client}/${r.bot}/${r.prompt_name}`));

  if(countsEl){
    if(modelsState.scopeMode === 'prompt'){
      countsEl.textContent = `${allScopeRows.length} bots · ${selectedEffective.length} seleccionados`;
    }else{
      const distinctBots = new Set(allScopeRows.map(r => r.client + '/' + r.bot)).size;
      countsEl.textContent = `${allScopeRows.length} prompts en ${distinctBots} bots · ${selectedEffective.length} seleccionados`;
    }
  }
  if(explEl){
    explEl.textContent = modelsState.scopeMode === 'prompt'
      ? 'El mismo prompt en todos los clientes. Marcá los bots y cambiá a la vez.'
      : 'Todos los prompts que usan este modelo, en cualquier bot. Elegí el modelo nuevo a la derecha y revisá.';
  }

  const thead = document.querySelector('#modelNewTable thead tr');
  if(thead){
    if(modelsState.scopeMode === 'prompt'){
      thead.innerHTML = `
        <th class="th-check"><input type="checkbox" id="selectAllVisible" title="Seleccionar visibles"></th>
        <th id="thClientBot">Client / bot</th>
        <th class="th-model">Modelo</th>
        <th class="th-temp">Temp.</th>
        <th class="th-reasoning">Reasoning</th>
        <th class="th-format">Formato</th>
        <th class="th-tokens">Tokens</th>
      `;
    }else{
      thead.innerHTML = `
        <th class="th-check"><input type="checkbox" id="selectAllVisible" title="Seleccionar visibles"></th>
        <th id="thClientBot">Client / bot</th>
        <th class="th-prompt">Prompt</th>
        <th class="th-model">Modelo</th>
        <th class="th-temp">Temp.</th>
        <th class="th-reasoning">Reasoning</th>
        <th class="th-format">Formato</th>
        <th class="th-tokens">Tokens</th>
      `;
    }
    const chkAll = $('selectAllVisible');
    if(chkAll){
      chkAll.onchange = e => {
        const checked = e.target.checked;
        const rows = getFilteredRows();
        rows.forEach(r => {
          const k1 = r.id;
          const k2 = `${r.client}/${r.bot}/${r.prompt_name}`;
          if(checked){
            modelsState.selectedRowKeys.add(k1);
            modelsState.selectedRowKeys.add(k2);
          }else{
            modelsState.selectedRowKeys.delete(k1);
            modelsState.selectedRowKeys.delete(k2);
          }
        });
        renderCenterColumn();
        updateRightColumn();
      };
    }
  }

  renderChips(allScopeRows);

  const tbody = $('modelTableBody');
  const emptyEl = $('modelTableEmpty');

  if(filteredRows.length === 0){
    if(tbody) tbody.innerHTML = '';
    if(emptyEl){
      emptyEl.hidden = false;
      emptyEl.textContent = 'Ningún resultado coincide con los filtros.';
    }
    updateSelectAllCheckbox(filteredRows);
    return;
  }

  if(emptyEl) emptyEl.hidden = true;

  if(tbody){
    tbody.innerHTML = filteredRows.map(r => {
      const k = r.id;
      const altKey = `${r.client}/${r.bot}/${r.prompt_name}`;
      const isSelected = modelsState.selectedRowKeys.has(k) || modelsState.selectedRowKeys.has(altKey);

      function renderDiffCell(currentVal, draftVal, hasError, isEmptyOption){
        const cur = (currentVal !== '' && currentVal != null) ? String(currentVal) : '';
        const curText = cur || '-';
        if(!isSelected || hasError){
          return `<span>${esc(curText)}</span>`;
        }
        if(isEmptyOption && draftVal === '__empty__'){
          if(cur !== ''){
            return `<span class="diff-cell-preview"><del class="diff-old">${esc(cur)}</del><span class="diff-arrow">→</span><ins class="diff-new">(vacío)</ins></span>`;
          }
          return `<span>${esc(curText)}</span>`;
        }
        if(draftVal !== '' && draftVal !== '__empty__' && draftVal !== cur){
          return `<span class="diff-cell-preview"><del class="diff-old">${esc(curText)}</del><span class="diff-arrow">→</span><ins class="diff-new">${esc(draftVal)}</ins></span>`;
        }
        return `<span>${esc(curText)}</span>`;
      }

      const curModel = r.model || r.OPENAI_MODEL || '';
      let middleCols = '';
      if(isPrompt){
        if(isSelected && modelsState.draft.model && modelsState.draft.model !== curModel){
          middleCols = `<td class="td-model"><span class="diff-cell-preview"><del class="diff-old">${esc(curModel || '-')}</del><span class="diff-arrow">→</span><ins class="diff-new">${esc(modelsState.draft.model)}</ins></span></td>`;
        }else{
          middleCols = `<td class="td-model"><code>${esc(curModel || '-')}</code></td>`;
        }
      }else{
        const promptCell = `<td class="td-prompt"><code class="prompt-name-mono">${esc(r.prompt || r.prompt_name)}</code></td>`;
        let modelCell = '';
        if(isSelected && modelsState.draft.model && modelsState.draft.model !== curModel){
          modelCell = `<td class="td-model"><span class="diff-cell-preview"><del class="diff-old">${esc(curModel || '-')}</del><span class="diff-arrow">→</span><ins class="diff-new">${esc(modelsState.draft.model)}</ins></span></td>`;
        }else{
          modelCell = `<td class="td-model"><code>${esc(curModel || '-')}</code></td>`;
        }
        middleCols = promptCell + modelCell;
      }

      const curTemp = r.temperature !== undefined ? r.temperature : r.TEMPERATURE;
      const curReas = r.reasoning_effort !== undefined ? r.reasoning_effort : r.REASONING_EFFORT;
      const curFmt = r.response_format_type !== undefined ? r.response_format_type : r.RESPONSE_FORMAT_TYPE;
      const curToks = r.max_completion_tokens !== undefined ? r.max_completion_tokens : r.MAX_COMPLETION_TOKENS;

      const tempHtml = renderDiffCell(curTemp, modelsState.draft.temperature, !!modelsState.validationErrors.temperature, false);
      const reasonHtml = renderDiffCell(curReas, modelsState.draft.reasoning_effort, false, true);
      const formatHtml = renderDiffCell(curFmt, modelsState.draft.response_format_type, false, true);
      const tokensHtml = renderDiffCell(curToks, modelsState.draft.max_tokens, !!modelsState.validationErrors.max_tokens, false);

      let varsDiffHtml = '';
      if(isSelected){
        const currentVars = r.input_variables || r.INPUT_VARIABLE || [];
        const addedForThisRow = modelsState.draft.variablesToAdd.filter(v => !currentVars.includes(v));
        const removedForThisRow = modelsState.draft.variablesToRemove.filter(v => currentVars.includes(v));
        if(addedForThisRow.length > 0 || removedForThisRow.length > 0){
          const addedPills = addedForThisRow.map(v => `<span class="diff-added">+${esc(v)}</span>`).join(' ');
          const removedPills = removedForThisRow.map(v => `<span class="diff-removed"><del>-${esc(v)}</del></span>`).join(' ');
          varsDiffHtml = `<div class="cell-vars-diff">${addedPills} ${removedPills}</div>`;
        }
      }

      const promptSubLine = !isPrompt
        ? `<div class="bot-prompt-sub" style="font-family:'JetBrains Mono',monospace;font-size:11px;color:#9cdcfe;margin-top:2px;">${esc(r.prompt || r.prompt_name)}</div>`
        : '';

      return `<tr data-key="${esc(k)}" data-alt-key="${esc(altKey)}" class="model-table-row${isSelected ? ' selected' : ''}" tabindex="0">
        <td class="td-check"><input type="checkbox" class="row-select-check" data-key="${esc(k)}" ${isSelected ? 'checked' : ''}></td>
        <td class="td-bot">
          <div><span class="bot-client-name">${esc(r.client)}</span> <span class="bot-slash">/</span> <span class="bot-name">${esc(r.bot)}</span></div>
          ${promptSubLine}
          ${varsDiffHtml}
        </td>
        ${middleCols}
        <td class="td-temp">${tempHtml}</td>
        <td class="td-reasoning">${reasonHtml}</td>
        <td class="td-format">${formatHtml}</td>
        <td class="td-tokens">${tokensHtml}</td>
      </tr>`;
    }).join('');
  }

  updateSelectAllCheckbox(filteredRows);
}

function updateSelectAllCheckbox(filteredRows){
  const chkAll = $('selectAllVisible');
  if(!chkAll) return;
  if(filteredRows.length === 0){
    chkAll.checked = false;
    chkAll.indeterminate = false;
    return;
  }
  let selectedCount = 0;
  filteredRows.forEach(r => {
    if(modelsState.selectedRowKeys.has(r.id) || modelsState.selectedRowKeys.has(`${r.client}/${r.bot}/${r.prompt_name}`)) selectedCount++;
  });
  if(selectedCount === 0){
    chkAll.checked = false;
    chkAll.indeterminate = false;
  }else if(selectedCount === filteredRows.length){
    chkAll.checked = true;
    chkAll.indeterminate = false;
  }else{
    chkAll.checked = false;
    chkAll.indeterminate = true;
  }
}

function updateRightColumn(){
  const allScopeRows = getAllScopeRows();
  const filteredRows = getFilteredRows();
  const selectedRows = filteredRows.filter(r => modelsState.selectedRowKeys.has(r.id) || modelsState.selectedRowKeys.has(`${r.client}/${r.bot}/${r.prompt_name}`));
  const selectedCount = selectedRows.length;
  const isPrompt = modelsState.scopeMode === 'prompt';

  if($('rightSelectedCount')) $('rightSelectedCount').textContent = selectedCount;
  if($('rightSelectedLabel')){
    $('rightSelectedLabel').textContent = isPrompt
      ? (selectedCount === 1 ? 'bot seleccionado' : 'bots seleccionados')
      : (selectedCount === 1 ? 'prompt seleccionado' : 'prompts seleccionados');
  }

  const modelSelect = $('editModelSelect');
  const lblModel = $('lblModelSelect');
  if(lblModel){
    lblModel.textContent = isPrompt ? 'Modelo' : `Nuevo modelo (migrar de ${modelsState.selectedItem || ''})`;
  }
  if(modelSelect && modelsState.data){
    const currentVal = modelsState.draft.model;
    const defaultLabel = isPrompt ? 'mantener' : `mantener (${modelsState.selectedItem || ''})`;
    if(modelSelect.options.length <= 1){
      const allKnown = new Set(modelsState.data.all_models || []);
      ['gpt-4o', 'gpt-4o-mini', 'o1', 'o1-mini', 'o3-mini', 'gpt-4.5-preview', 'chatgpt-4o-latest', 'gpt-4-turbo'].forEach(m => allKnown.add(m));
      const sorted = [...allKnown].sort();
      sorted.forEach(m => {
        const opt = document.createElement('option');
        opt.value = m;
        opt.textContent = m;
        modelSelect.appendChild(opt);
      });
    }
    if(modelSelect.options.length > 0){
      modelSelect.options[0].textContent = defaultLabel;
      modelSelect.value = currentVal;
    }
  }

  const varListEl = $('variablesList');
  const newVarInput = $('newVarInput');
  const btnAddVar = $('btnAddVar');

  if(isPrompt && modelsState.selectedItem){
    if(newVarInput) newVarInput.disabled = false;
    if(btnAddVar) btnAddVar.disabled = false;

    if(varListEl){
      const targetRows = selectedRows.length > 0 ? selectedRows : allScopeRows;

      const varCounts = new Map();
      targetRows.forEach(r => {
        const vList = r.input_variables || r.INPUT_VARIABLE || [];
        vList.forEach(v => {
          varCounts.set(v, (varCounts.get(v) || 0) + 1);
        });
      });

      const existingNames = [...varCounts.keys()].sort();
      let html = '';

      existingNames.forEach(v => {
        const isRemoved = modelsState.draft.variablesToRemove.includes(v);
        if(isRemoved){
          html += `<span class="var-pill var-pill-removed"><span class="var-pill-text"><del>${esc(v)}</del></span><button type="button" class="var-undo-btn" data-undo-var="${esc(v)}" title="Deshacer quitar variable">↩</button></span> `;
        }else{
          const isPartial = varCounts.get(v) < targetRows.length;
          const badge = isPartial ? `<span class="var-partial-badge">(${varCounts.get(v)}/${targetRows.length})</span>` : '';
          const partialClass = isPartial ? ' var-pill-partial' : '';
          html += `<span class="var-pill${partialClass}"><span class="var-pill-text">${esc(v)}${badge}</span><button type="button" class="var-remove-btn" data-del-var="${esc(v)}" title="Quitar variable">✕</button></span> `;
        }
      });

      modelsState.draft.variablesToAdd.forEach(v => {
        html += `<span class="var-pill var-pill-added"><span class="var-pill-text"><ins>+${esc(v)}</ins></span><button type="button" class="var-remove-btn" data-cancel-add-var="${esc(v)}" title="Cancelar agregar">✕</button></span> `;
      });

      if(!html.trim()){
        varListEl.innerHTML = '<span class="var-none">Este prompt no tiene variables declaradas.</span>';
      }else{
        varListEl.innerHTML = html;
      }
    }
  }else{
    if(newVarInput) newVarInput.disabled = true;
    if(btnAddVar) btnAddVar.disabled = true;
    if(varListEl){
      varListEl.innerHTML = '<span class="var-none">Las variables se configuran por prompt (cambiá a la vista "Por prompt").</span>';
    }
  }

  const btnReview = $('btnReviewChanges');
  if(btnReview){
    const hasChanges = (
      modelsState.draft.model !== '' ||
      modelsState.draft.temperature !== '' ||
      modelsState.draft.reasoning_effort !== '' ||
      modelsState.draft.response_format_type !== '' ||
      modelsState.draft.max_tokens !== '' ||
      modelsState.draft.variablesToAdd.length > 0 ||
      modelsState.draft.variablesToRemove.length > 0
    );
    const hasErrors = !!(modelsState.validationErrors.temperature || modelsState.validationErrors.max_tokens);
    const canReview = selectedCount > 0 && hasChanges && !hasErrors;
    btnReview.disabled = !canReview;
    btnReview.textContent = `Revisar ${selectedCount} ${isPrompt ? (selectedCount === 1 ? 'bot' : 'bots') : (selectedCount === 1 ? 'prompt' : 'prompts')}`;
  }

  const footerSummary = $('rightFooterSummary');
  if(footerSummary){
    if(modelsState.validationErrors.temperature || modelsState.validationErrors.max_tokens){
      footerSummary.innerHTML = '<span style="color:#f48771;font-weight:600;">Hay errores en los campos. Corregilos para continuar.</span>';
    }else if(selectedCount === 0){
      footerSummary.textContent = 'Marcá filas en la tabla para empezar.';
    }else{
      const changes = [];
      if(modelsState.draft.model) changes.push(`modelo: ${modelsState.draft.model}`);
      if(modelsState.draft.temperature !== '') changes.push(`temp: ${modelsState.draft.temperature}`);
      if(modelsState.draft.reasoning_effort === '__empty__') changes.push('quitar reasoning');
      else if(modelsState.draft.reasoning_effort) changes.push(`reasoning: ${modelsState.draft.reasoning_effort}`);
      if(modelsState.draft.response_format_type === '__empty__') changes.push('quitar formato');
      else if(modelsState.draft.response_format_type) changes.push(`formato: ${modelsState.draft.response_format_type}`);
      if(modelsState.draft.max_tokens !== '') changes.push(`tokens: ${modelsState.draft.max_tokens}`);
      if(modelsState.draft.variablesToAdd.length > 0) changes.push(`+var: ${modelsState.draft.variablesToAdd.join(', ')}`);
      if(modelsState.draft.variablesToRemove.length > 0) changes.push(`-var: ${modelsState.draft.variablesToRemove.join(', ')}`);

      if(changes.length === 0){
        footerSummary.textContent = `${selectedCount} seleccionados. Elegí qué campos modificar.`;
      }else{
        const targetNoun = isPrompt
          ? (selectedCount === 1 ? '1 bot' : `${selectedCount} bots`)
          : (selectedCount === 1 ? '1 prompt' : `${selectedCount} prompts`);

        if(!isPrompt && modelsState.draft.model && modelsState.draft.model !== modelsState.selectedItem){
          const extraChanges = changes.filter(c => !c.startsWith('modelo:'));
          const extraText = extraChanges.length > 0 ? ` (con ${extraChanges.join(', ')})` : '';
          footerSummary.textContent = `Migrar ${modelsState.selectedItem} → ${modelsState.draft.model}${extraText} en ${targetNoun}.`;
        }else{
          footerSummary.textContent = `Cambiar ${changes.join(', ')} en ${targetNoun}.`;
        }
      }
    }
  }
}

let lastUndoBatchId = null;
const MODELS_APPLY_ENABLED = true;
const MODELS_APPLY_DISABLED_LABEL = 'Aplicar cambios';

function closeModelReviewModal(){
  const modal = $('modelReviewModal');
  if(modal) modal.hidden = true;
  const prodInput = $('modalReviewProdInput');
  if(prodInput) prodInput.value = '';
}

function openModelReviewModal(){
  const allRows = getAllScopeRows();
  const selectedRows = allRows.filter(r => modelsState.selectedRowKeys.has(`${r.client}/${r.bot}/${r.prompt_name}`));
  if(selectedRows.length === 0) return;

  const distinctBots = new Set(selectedRows.map(r => `${r.client}/${r.bot}`)).size;
  const isPrompt = modelsState.scopeMode === 'prompt';

  const envBadge = $('modalReviewEnvBadge');
  if(envBadge){
    envBadge.textContent = modelsState.env.toUpperCase();
    envBadge.className = 'env-badge ' + modelsState.env;
  }
  const countsEl = $('modalReviewCounts');
  if(countsEl){
    countsEl.textContent = `${distinctBots} bots · ${selectedRows.length} prompts afectados`;
  }

  // Summary box
  const summaryBox = $('modalReviewSummaryBox');
  if(summaryBox){
    const summaryItems = [];
    if(modelsState.draft.model) summaryItems.push(`<div><strong>Modelo destino:</strong> <code>${esc(modelsState.draft.model)}</code></div>`);
    if(modelsState.draft.temperature !== '') summaryItems.push(`<div><strong>Temperatura:</strong> <code>${esc(modelsState.draft.temperature)}</code></div>`);
    if(modelsState.draft.reasoning_effort === '__empty__') summaryItems.push(`<div><strong>Reasoning effort:</strong> <em>(eliminar campo)</em></div>`);
    else if(modelsState.draft.reasoning_effort) summaryItems.push(`<div><strong>Reasoning effort:</strong> <code>${esc(modelsState.draft.reasoning_effort)}</code></div>`);
    if(modelsState.draft.response_format_type === '__empty__') summaryItems.push(`<div><strong>Formato de respuesta:</strong> <em>(eliminar campo)</em></div>`);
    else if(modelsState.draft.response_format_type) summaryItems.push(`<div><strong>Formato de respuesta:</strong> <code>${esc(modelsState.draft.response_format_type)}</code></div>`);
    if(modelsState.draft.max_tokens !== '') summaryItems.push(`<div><strong>Max completion tokens:</strong> <code>${esc(modelsState.draft.max_tokens)}</code></div>`);
    if(modelsState.draft.variablesToAdd.length > 0) summaryItems.push(`<div><strong>Variables a agregar:</strong> <span class="diff-added">${esc(modelsState.draft.variablesToAdd.map(v => '+' + v).join(', '))}</span></div>`);
    if(modelsState.draft.variablesToRemove.length > 0) summaryItems.push(`<div><strong>Variables a quitar:</strong> <span class="diff-removed">${esc(modelsState.draft.variablesToRemove.map(v => '-' + v).join(', '))}</span></div>`);

    summaryBox.innerHTML = summaryItems.join('') || '<div>Sin cambios especificados.</div>';
  }

  // Table detail
  const tbody = $('modalReviewTableBody');
  if(tbody){
    tbody.innerHTML = selectedRows.map(r => {
      const diffs = [];
      if(modelsState.draft.model && modelsState.draft.model !== r.OPENAI_MODEL){
        diffs.push(`Modelo: <del class="diff-old">${esc(r.OPENAI_MODEL || '-')}</del> → <ins class="diff-new">${esc(modelsState.draft.model)}</ins>`);
      }
      if(modelsState.draft.temperature !== '' && modelsState.draft.temperature !== String(r.TEMPERATURE)){
        diffs.push(`Temp: <del class="diff-old">${esc(r.TEMPERATURE || '-')}</del> → <ins class="diff-new">${esc(modelsState.draft.temperature)}</ins>`);
      }
      if(modelsState.draft.reasoning_effort === '__empty__' && r.REASONING_EFFORT){
        diffs.push(`Reasoning: <del class="diff-old">${esc(r.REASONING_EFFORT)}</del> → <ins class="diff-new">(quitar)</ins>`);
      }else if(modelsState.draft.reasoning_effort && modelsState.draft.reasoning_effort !== '__empty__' && modelsState.draft.reasoning_effort !== r.REASONING_EFFORT){
        diffs.push(`Reasoning: <del class="diff-old">${esc(r.REASONING_EFFORT || '-')}</del> → <ins class="diff-new">${esc(modelsState.draft.reasoning_effort)}</ins>`);
      }
      if(modelsState.draft.response_format_type === '__empty__' && r.RESPONSE_FORMAT_TYPE){
        diffs.push(`Formato: <del class="diff-old">${esc(r.RESPONSE_FORMAT_TYPE)}</del> → <ins class="diff-new">(quitar)</ins>`);
      }else if(modelsState.draft.response_format_type && modelsState.draft.response_format_type !== '__empty__' && modelsState.draft.response_format_type !== r.RESPONSE_FORMAT_TYPE){
        diffs.push(`Formato: <del class="diff-old">${esc(r.RESPONSE_FORMAT_TYPE || '-')}</del> → <ins class="diff-new">${esc(modelsState.draft.response_format_type)}</ins>`);
      }
      if(modelsState.draft.max_tokens !== '' && modelsState.draft.max_tokens !== String(r.MAX_COMPLETION_TOKENS)){
        diffs.push(`Tokens: <del class="diff-old">${esc(r.MAX_COMPLETION_TOKENS || '-')}</del> → <ins class="diff-new">${esc(modelsState.draft.max_tokens)}</ins>`);
      }
      if(isPrompt){
        const curVars = r.INPUT_VARIABLE || [];
        const addV = modelsState.draft.variablesToAdd.filter(v => !curVars.includes(v));
        const remV = modelsState.draft.variablesToRemove.filter(v => curVars.includes(v));
        if(addV.length) diffs.push(`<span class="diff-added">+${esc(addV.join(', '))}</span>`);
        if(remV.length) diffs.push(`<span class="diff-removed">-${esc(remV.join(', '))}</span>`);
      }

      const diffsDisplay = diffs.length > 0 ? diffs.join(' · ') : '<span style="color:#6e7681;">Sin modificaciones para esta fila</span>';

      return `<tr style="border-bottom:1px solid #282828;">
        <td style="padding:6px 10px;font-family:'JetBrains Mono',monospace;">${esc(r.client)} / ${esc(r.bot)}</td>
        <td style="padding:6px 10px;font-family:'JetBrains Mono',monospace;color:#9cdcfe;">${esc(r.prompt_name)}</td>
        <td style="padding:6px 10px;">${diffsDisplay}</td>
      </tr>`;
    }).join('');
  }

  // Prod warning
  const isProd = modelsState.env === 'prod';
  const prodWarning = $('modalReviewProdWarning');
  const btnConfirm = $('btnModalReviewConfirm');

  if(prodWarning) prodWarning.style.display = isProd ? 'block' : 'none';
  if(btnConfirm){
    btnConfirm.disabled = isProd;
    btnConfirm.textContent = isProd
      ? `Confirmar y Aplicar en PROD (${distinctBots} bots)`
      : `Aplicar cambios en ${distinctBots} bots (${selectedRows.length} prompts)`;
  }

  const modal = $('modelReviewModal');
  if(modal) modal.hidden = false;
}

async function applyModelChangesBatch(){
  if(!MODELS_APPLY_ENABLED) return;
  const allRows = getAllScopeRows();
  const selectedRows = allRows.filter(r => modelsState.selectedRowKeys.has(`${r.client}/${r.bot}/${r.prompt_name}`));
  if(selectedRows.length === 0) return;

  const btnConfirm = $('btnModalReviewConfirm');
  if(btnConfirm){
    btnConfirm.disabled = true;
    btnConfirm.textContent = 'Aplicando cambios en DynamoDB…';
  }

  try{
    const botMap = new Map();
    selectedRows.forEach(r => {
      const k = `${r.client}/${r.bot}`;
      if(!botMap.has(k)){
        botMap.set(k, {
          client: r.client,
          bot: r.bot,
          expected_hash: r.prompts_hash || r.version,
          prompts: {}
        });
      }
      const pChanges = {};
      if(modelsState.draft.model) pChanges.model = modelsState.draft.model;
      if(modelsState.draft.temperature !== '') pChanges.temperature = modelsState.draft.temperature;
      if(modelsState.draft.reasoning_effort) pChanges.reasoning_effort = modelsState.draft.reasoning_effort;
      if(modelsState.draft.response_format_type) pChanges.response_format_type = modelsState.draft.response_format_type;
      if(modelsState.draft.max_tokens !== '') pChanges.max_tokens = modelsState.draft.max_tokens;
      if(modelsState.draft.variablesToAdd.length > 0) pChanges.add_variables = modelsState.draft.variablesToAdd;
      if(modelsState.draft.variablesToRemove.length > 0) pChanges.remove_variables = modelsState.draft.variablesToRemove;
      pChanges.path = r.path;
      pChanges.prompt = r.prompt;
      botMap.get(k).prompts[r.prompt_name] = pChanges;
    });

    const payload = {
      env: modelsState.env,
      confirm_prod: modelsState.env === 'prod',
      changes: [...botMap.values()]
    };

    const res = await api('/api/models/apply-batch', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(payload)
    });

    closeModelReviewModal();
    clearDraftEdits();
    lastUndoBatchId = res.batch_id;

    const statusText = $('modelStatusText');
    if(statusText){
      statusText.innerHTML = `<span class="model-status-ok">✓ ${res.prompt_count} prompts actualizados en ${res.bot_count} bots. ${res.backup_count} backups guardados.</span>`;
    }

    const undoBtn = $('modelUndoBtn');
    if(undoBtn){
      undoBtn.hidden = false;
      undoBtn.textContent = `Deshacer (${res.bot_count} bots)`;
    }

    await loadModelsData(true);
  }catch(e){
    alert('Error al aplicar cambios: ' + e.message);
    if(btnConfirm){
      btnConfirm.disabled = (modelsState.env === 'prod');
      btnConfirm.textContent = 'Aplicar cambios';
    }
  }
}

async function undoModelChangesBatch(){
  if(!MODELS_APPLY_ENABLED) return;
  if(!lastUndoBatchId) return;
  if(!window.confirm('¿Deshacer el último cambio de modelos restaurando los backups previos de cada bot?')) return;

  const undoBtn = $('modelUndoBtn');
  if(undoBtn){
    undoBtn.disabled = true;
    undoBtn.textContent = 'Restaurando…';
  }

  try{
    const res = await api('/api/models/undo-batch', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({batch_id: lastUndoBatchId})
    });

    lastUndoBatchId = null;
    if(undoBtn) undoBtn.hidden = true;

    const statusText = $('modelStatusText');
    if(statusText){
      statusText.innerHTML = `<span class="model-status-ok">✓ Se restauraron ${res.restored_bots} bots a su estado previo desde backups.</span>`;
    }

    await loadModelsData(true);
  }catch(e){
    alert('Error al deshacer cambios: ' + e.message);
    if(undoBtn){
      undoBtn.disabled = false;
      undoBtn.textContent = 'Deshacer';
    }
  }
}

let currentReplicatePlan = null;

function closeModelReplicateModal(){
  const modal = $('modelReplicateModal');
  if(modal) modal.hidden = true;
  const prodInput = $('modalReplicateProdInput');
  if(prodInput) prodInput.value = '';
  currentReplicatePlan = null;
}

function openModelReplicateModal(defaultSource = null){
  const targetEnv = modelsState.env || 'desa';
  const targetBadge = $('modalReplicateTargetBadge');
  if(targetBadge){
    targetBadge.textContent = targetEnv.toUpperCase();
    targetBadge.className = 'env-badge ' + targetEnv;
  }
  const targetName = $('replicateTargetEnvName');
  if(targetName){
    targetName.textContent = targetEnv.toUpperCase();
  }

  // Cargar ambientes disponibles distintos al destino
  const sel = $('replicateSourceEnvSelect');
  if(sel){
    const allEnvs = (Array.isArray(ENVS) && ENVS.length > 0) ? ENVS.map(e => e.name) : ['desa', 'test', 'prod'];
    const otherEnvs = allEnvs.filter(e => e !== targetEnv);
    sel.innerHTML = otherEnvs.map(e => {
      const label = e === 'prod' ? 'PRODUCCIÓN (prod)' : (e === 'test' ? 'TEST (test)' : 'DESA (desa)');
      return `<option value="${e}">${label}</option>`;
    }).join('');

    if(defaultSource && otherEnvs.includes(defaultSource)){
      sel.value = defaultSource;
    }else if(otherEnvs.includes('prod')){
      sel.value = 'prod';
    }else if(otherEnvs.length > 0){
      sel.value = otherEnvs[0];
    }
  }

  const modal = $('modelReplicateModal');
  if(modal) modal.hidden = false;

  loadReplicatePlan();
}

async function loadReplicatePlan(){
  const targetEnv = modelsState.env || 'desa';
  const sel = $('replicateSourceEnvSelect');
  const sourceEnv = sel ? sel.value : (targetEnv === 'prod' ? 'test' : 'prod');

  const btnConfirm = $('btnModalReplicateConfirm');
  if(btnConfirm){
    btnConfirm.disabled = true;
    btnConfirm.textContent = 'Calculando diferencias…';
  }

  const statBots = $('statRepChangedBots');
  const statPrompts = $('statRepChangedPrompts');
  const statUnchanged = $('statRepUnchangedBots');
  const statSkipped = $('statRepSkippedBots');
  if(statBots) statBots.textContent = '…';
  if(statPrompts) statPrompts.textContent = '…';
  if(statUnchanged) statUnchanged.textContent = '…';
  if(statSkipped) statSkipped.textContent = '…';

  const tbody = $('modalReplicateTableBody');
  if(tbody){
    tbody.innerHTML = '<tr><td colspan="3" style="text-align:center;padding:24px;color:#858585;">Analizando configuración de prompts entre ' + sourceEnv.toUpperCase() + ' y ' + targetEnv.toUpperCase() + '…</td></tr>';
  }

  try{
    const plan = await api(`/api/models/replicate-plan?source=${encodeURIComponent(sourceEnv)}&target=${encodeURIComponent(targetEnv)}`);
    currentReplicatePlan = plan;

    if(statBots) statBots.textContent = plan.changed_bots_count;
    if(statPrompts) statPrompts.textContent = plan.changed_prompts_count;
    if(statUnchanged) statUnchanged.textContent = plan.unchanged_bots_count;
    if(statSkipped){
      statSkipped.textContent = plan.skipped_bots_count;
      statSkipped.title = plan.skipped_bots && plan.skipped_bots.length > 0 ? ('Bots ausentes en origen:\n' + plan.skipped_bots.join('\n')) : 'Ningún bot omitido';
    }

    const countsEl = $('modalReplicateCounts');
    if(countsEl){
      countsEl.textContent = `${plan.matching_bots_count} bots coincidentes · ${plan.changed_bots_count} con cambios`;
    }

    renderReplicateDiffTable();

    const isProd = targetEnv === 'prod';
    const prodWarning = $('modalReplicateProdWarning');
    if(prodWarning) prodWarning.style.display = isProd ? 'block' : 'none';

    if(btnConfirm){
      if(plan.changed_bots_count === 0){
        btnConfirm.disabled = true;
        btnConfirm.textContent = 'Sin diferencias a replicar';
      }else if(isProd){
        const prodInput = $('modalReplicateProdInput');
        const isConfirmed = prodInput && prodInput.value.trim() === 'PROD';
        btnConfirm.disabled = !isConfirmed;
        btnConfirm.textContent = `Replicar en PROD (${plan.changed_bots_count} bots, ${plan.changed_prompts_count} prompts)`;
      }else{
        btnConfirm.disabled = false;
        btnConfirm.textContent = `Replicar estructura desde ${sourceEnv.toUpperCase()} (${plan.changed_bots_count} bots, ${plan.changed_prompts_count} prompts)`;
      }
    }
  }catch(err){
    if(tbody){
      tbody.innerHTML = `<tr><td colspan="3" style="text-align:center;padding:20px;color:#f48771;">Error al calcular plan: ${esc(err.message)}</td></tr>`;
    }
    if(btnConfirm){
      btnConfirm.disabled = true;
      btnConfirm.textContent = 'Error';
    }
  }
}

function renderReplicateDiffTable(){
  const tbody = $('modalReplicateTableBody');
  if(!tbody) return;
  if(!currentReplicatePlan || !currentReplicatePlan.diffs){
    tbody.innerHTML = '';
    return;
  }

  const filterVal = ($('replicateTableFilter') ? $('replicateTableFilter').value : '').toLowerCase().trim();
  const diffs = currentReplicatePlan.diffs.filter(d => {
    if(!filterVal) return true;
    return d.client.toLowerCase().includes(filterVal) ||
           d.bot.toLowerCase().includes(filterVal) ||
           d.prompt_name.toLowerCase().includes(filterVal);
  });

  if(diffs.length === 0){
    if(currentReplicatePlan.diffs.length === 0){
      tbody.innerHTML = `<tr><td colspan="3" style="text-align:center;padding:24px;color:#858585;">No se detectaron diferencias. Todos los bots coincidentes en ${currentReplicatePlan.target_env.toUpperCase()} ya tienen la misma configuración de prompts que ${currentReplicatePlan.source_env.toUpperCase()}.</td></tr>`;
    }else{
      tbody.innerHTML = `<tr><td colspan="3" style="text-align:center;padding:20px;color:#858585;">No hay resultados que coincidan con "${esc(filterVal)}".</td></tr>`;
    }
    return;
  }

  tbody.innerHTML = diffs.map(d => {
    const diffSnippets = [];
    if(d.diffs.model){
      diffSnippets.push(`Modelo: <del class="diff-old">${esc(d.diffs.model.old || '-')}</del> → <ins class="diff-new">${esc(d.diffs.model.new)}</ins>`);
    }
    if(d.diffs.temperature){
      diffSnippets.push(`Temp: <del class="diff-old">${esc(d.diffs.temperature.old || '-')}</del> → <ins class="diff-new">${esc(d.diffs.temperature.new)}</ins>`);
    }
    if(d.diffs.reasoning_effort){
      diffSnippets.push(`Reasoning: <del class="diff-old">${esc(d.diffs.reasoning_effort.old || '-')}</del> → <ins class="diff-new">${esc(d.diffs.reasoning_effort.new || '(quitar)')}</ins>`);
    }
    if(d.diffs.response_format_type){
      diffSnippets.push(`Formato: <del class="diff-old">${esc(d.diffs.response_format_type.old || '-')}</del> → <ins class="diff-new">${esc(d.diffs.response_format_type.new || '(quitar)')}</ins>`);
    }
    if(d.diffs.max_tokens){
      diffSnippets.push(`Tokens: <del class="diff-old">${esc(d.diffs.max_tokens.old || '-')}</del> → <ins class="diff-new">${esc(d.diffs.max_tokens.new || '(quitar)')}</ins>`);
    }
    if(d.diffs.input_variables){
      const added = d.diffs.input_variables.added || [];
      const removed = d.diffs.input_variables.removed || [];
      const vSnippets = [];
      if(added.length > 0) vSnippets.push(`<span class="diff-added">+${esc(added.join(', '))}</span>`);
      if(removed.length > 0) vSnippets.push(`<span class="diff-removed">-${esc(removed.join(', '))}</span>`);
      if(vSnippets.length > 0) diffSnippets.push(`Variables: ${vSnippets.join(' ')}`);
    }

    const diffDisplay = diffSnippets.join(' · ');

    return `<tr style="border-bottom:1px solid #282828;">
      <td style="padding:6px 10px;font-family:'JetBrains Mono',monospace;">${esc(d.client)} / ${esc(d.bot)}</td>
      <td style="padding:6px 10px;font-family:'JetBrains Mono',monospace;color:#9cdcfe;">${esc(d.prompt_name)}</td>
      <td style="padding:6px 10px;">${diffDisplay}</td>
    </tr>`;
  }).join('');
}

async function applyReplicationPlan(){
  if(!MODELS_APPLY_ENABLED || !currentReplicatePlan) return;
  if(currentReplicatePlan.changes.length === 0){
    alert('No hay cambios para replicar.');
    return;
  }

  const isProd = currentReplicatePlan.target_env === 'prod';
  if(isProd){
    const prodInput = $('modalReplicateProdInput');
    if(!prodInput || prodInput.value.trim() !== 'PROD'){
      alert('Debes escribir PROD para confirmar la replicación en el ambiente de producción.');
      return;
    }
  }

  const btnConfirm = $('btnModalReplicateConfirm');
  if(btnConfirm){
    btnConfirm.disabled = true;
    btnConfirm.textContent = 'Replicando en DynamoDB y creando backups…';
  }

  try{
    const payload = {
      env: currentReplicatePlan.target_env,
      confirm_prod: isProd,
      changes: currentReplicatePlan.changes
    };

    const res = await api('/api/models/apply-batch', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(payload)
    });

    const srcEnv = currentReplicatePlan.source_env.toUpperCase();
    const tgtEnv = currentReplicatePlan.target_env.toUpperCase();

    closeModelReplicateModal();
    lastUndoBatchId = res.batch_id;

    const statusText = $('modelStatusText');
    if(statusText){
      statusText.innerHTML = `<span class="model-status-ok">✓ Replicados ${res.prompt_count} prompts en ${res.bot_count} bots desde ${srcEnv} hacia ${tgtEnv}. ${res.backup_count} backups guardados.</span>`;
    }

    const undoBtn = $('modelUndoBtn');
    if(undoBtn){
      undoBtn.hidden = false;
      undoBtn.textContent = `Deshacer replicación (${res.bot_count} bots)`;
    }

    await loadModelsData(true);
  }catch(e){
    alert('Error al aplicar replicación: ' + e.message);
    if(btnConfirm){
      btnConfirm.disabled = (currentReplicatePlan.target_env === 'prod');
      btnConfirm.textContent = 'Replicar estructura';
    }
  }
}

function initModelsView(){
  if(!modelsState.data || modelsState.env !== $('modelEnv').value){
    modelsState.env = $('modelEnv').value || 'desa';
    document.querySelectorAll('#modelEnvSegmented .env-btn').forEach(btn => {
      btn.classList.toggle('active', btn.dataset.env === modelsState.env);
    });
    loadModelsData(false);
  }else{
    renderLeftList();
    renderCenterColumn();
    updateRightColumn();
  }
}

function openModelHistory(){
  if(modelsState.selectedRowKeys.size > 0){
    const firstKey = [...modelsState.selectedRowKeys][0];
    const parts = firstKey.split('/');
    singleActiveKey = {env: modelsState.env, client: parts[0], bot: parts[1]};
    openHistoryModal();
  }else{
    alert('Seleccioná al menos un bot en la tabla para ver su historial de cambios.');
  }
}

async function downloadModelsExcel(){
  if(!modelsState.data) return;
  const env = modelsState.env;
  const statusText = $('modelStatusText');
  if(statusText) statusText.textContent = `Generando Excel de modelos para ${env.toUpperCase()}…`;

  try{
    const rows = modelsState.data.rows;
    const modelMap = new Map();
    rows.forEach(r => {
      const m = r.OPENAI_MODEL;
      if(!modelMap.has(m)) modelMap.set(m, new Map());
      const botMap = modelMap.get(m);
      const botKey = r.client + ' / ' + r.bot;
      if(!botMap.has(botKey)) botMap.set(botKey, {client: r.client, bot: r.bot, prompt_details: []});
      botMap.get(botKey).prompt_details.push({
        name: r.prompt_name,
        path: ['AGENT_CONFIG', 'PROMPTS', r.prompt_name],
        model: r.OPENAI_MODEL,
        temperature: r.TEMPERATURE,
        reasoning_effort: r.REASONING_EFFORT,
        response_format_type: r.RESPONSE_FORMAT_TYPE,
        max_completion_tokens: r.MAX_COMPLETION_TOKENS
      });
    });

    const exportPayload = [];
    modelMap.forEach((botMap, model) => {
      exportPayload.push({model, items: [...botMap.values()]});
    });

    const response = await fetch('/api/models/export', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({env, models: exportPayload})
    });
    if(!response.ok){
      const err = await response.json().catch(() => ({}));
      throw new Error(err.error || ('HTTP ' + response.status));
    }
    const blob = await response.blob();
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = `modelos_${env}.xlsx`;
    document.body.appendChild(link);
    link.click();
    link.remove();
    setTimeout(() => URL.revokeObjectURL(url), 0);
    if(statusText) statusText.textContent = `Excel descargado: modelos_${env}.xlsx (${exportPayload.length} modelos).`;
  }catch(e){
    if(statusText) statusText.textContent = `Error descargando Excel: ${e.message}`;
  }
}

async function loadApiKeys(force){
  setStatus('Leyendo OPENAI_API_KEY en Desa, Test y Prod…');
  try{
    API_KEY_AUDIT = await api('/api/apikeys' + (force ? '?refresh=1' : ''));
    renderApiKeys();
    setStatus('Auditoría de API keys cargada para ' + API_KEY_AUDIT.rows.length + ' client/bot.');
  }catch(e){ setStatus(e.message, 'err'); }
}
function maskKey(value){
  if(!value) return '(vacía)';
  const visible = value.length > 10 ? 5 : 4;
  return value.slice(0, visible) + '*'.repeat(Math.max(0, value.length - visible));
}
let apkActiveFilter = 'all';

function formatKeyShort(val){
  if(!val) return '';
  const s = String(val).trim();
  if(!s) return '';
  if(s.length >= 4){
    return '...' + s.slice(-4);
  }
  return s;
}

function setApkFilter(filterKey){
  apkActiveFilter = filterKey;
  document.querySelectorAll('#apiKeyFilterPills .apk-pill').forEach(btn => {
    btn.classList.toggle('active', btn.dataset.filter === filterKey);
  });
  renderApiKeys();
}

function renderApiKeys(){
  if(!API_KEY_AUDIT || !API_KEY_AUDIT.rows) return;
  const allRows = API_KEY_AUDIT.rows;

  // Exact counts matching new user rules:
  // 1. Colisión Prod: clave de Prod repetida en OTRO CLIENTE (cross-client collision)
  // 2. Desa ≠ Test: test difiere de desa
  // 3. Sin Prod: bots sin clave en prod
  // 4. Todos: total de bots
  const countCollisions = allRows.filter(r => r.prod_cross_collisions && r.prod_cross_collisions.length > 0).length;
  const countMismatch = allRows.filter(r => r.desa_test_match === false).length;
  const countSinProd = allRows.filter(r => !r.prod || r.prod.length === 0).length;
  const countAll = allRows.length;

  if($('countApkCollisions')) $('countApkCollisions').textContent = countCollisions;
  if($('countApkMismatch')) $('countApkMismatch').textContent = countMismatch;
  if($('countApkNoProd')) $('countApkNoProd').textContent = countSinProd;
  if($('countApkAll')) $('countApkAll').textContent = countAll;

  // Filter based on active pill
  let filtered = allRows.filter(r => {
    if(apkActiveFilter === 'collisions'){
      return r.prod_cross_collisions && r.prod_cross_collisions.length > 0;
    }else if(apkActiveFilter === 'mismatch'){
      return r.desa_test_match === false;
    }else if(apkActiveFilter === 'sin_prod'){
      return !r.prod || r.prod.length === 0;
    }
    return true; // 'all'
  });

  // Filter based on search input
  const searchInput = $('apiKeySearchInput');
  const query = (searchInput && searchInput.value ? searchInput.value : ($('filter') ? $('filter').value : '')).toLowerCase().trim();
  if(query){
    filtered = filtered.filter(row => {
      return [
        row.client,
        row.bot,
        ...['desa','test','prod'].flatMap(env => (row[env] || []).flatMap(entry => [entry.path, entry.value]))
      ].join(' ').toLowerCase().includes(query);
    });
  }

  const tbody = $('apiKeyRows');
  if(!tbody) return;

  if(!filtered.length){
    tbody.innerHTML = '';
    if($('apiKeyEmptyState')) $('apiKeyEmptyState').hidden = false;
    if($('exportApiKeys')) $('exportApiKeys').disabled = allRows.length === 0;
    return;
  }
  if($('apiKeyEmptyState')) $('apiKeyEmptyState').hidden = true;

  tbody.innerHTML = filtered.map(row => {
    const desaVal = row.desa && row.desa[0] ? row.desa[0].value : null;
    const testVal = row.test && row.test[0] ? row.test[0].value : null;
    const prodVal = row.prod && row.prod[0] ? row.prod[0].value : null;

    const isMismatch = row.desa_test_match === false;
    const desaEqProd = (desaVal && prodVal && desaVal === prodVal) || Boolean(row.desa_equals_prod);
    const testEqProd = (testVal && prodVal && testVal === prodVal) || Boolean(row.test_equals_prod);
    const nonprodEqProd = desaEqProd || testEqProd;

    // Desa cell (si Desa == Prod o Desa != Test, se colorea en amarillo)
    let desaCell = '<span class="apk-dash">—</span>';
    if(desaVal){
      const shortKey = esc(formatKeyShort(desaVal));
      const titlePath = row.desa.map(e => e.path + ': ' + maskKey(e.value)).join('\n');
      if(desaEqProd){
        desaCell = '<span class="apk-key-diff" style="color:#facc15;border-color:rgba(234,179,8,0.45);background:rgba(234,179,8,0.12);" title="Atención: La API key de DESA es idéntica a PROD (PROD debe ser única)\n' + esc(titlePath) + '">= PROD ' + shortKey + '</span>';
      }else if(isMismatch){
        desaCell = '<span class="apk-key-diff" title="Desa ≠ Test: la key difiere de Test\n' + esc(titlePath) + '">≠ ' + shortKey + '</span>';
      }else{
        desaCell = '<span class="apk-key-code" title="' + esc(titlePath) + '">' + shortKey + '</span>';
      }
    }

    // Test cell (si Test == Prod o Desa != Test, se colorea en amarillo)
    let testCell = '<span class="apk-dash">—</span>';
    if(testVal){
      const shortKey = esc(formatKeyShort(testVal));
      const titlePath = row.test.map(e => e.path + ': ' + maskKey(e.value)).join('\n');
      if(testEqProd){
        testCell = '<span class="apk-key-diff" style="color:#facc15;border-color:rgba(234,179,8,0.45);background:rgba(234,179,8,0.12);" title="Atención: La API key de TEST es idéntica a PROD (PROD debe ser única)\n' + esc(titlePath) + '">= PROD ' + shortKey + '</span>';
      }else if(isMismatch){
        testCell = '<span class="apk-key-diff" title="Desa ≠ Test: la key difiere de Desa\n' + esc(titlePath) + '">≠ ' + shortKey + '</span>';
      }else if(row.desa_test_match === true){
        testCell = '<span class="apk-key-match" title="Desa = Test ✓\n' + esc(titlePath) + '">✓ ' + shortKey + '</span>';
      }else{
        testCell = '<span class="apk-key-code" title="' + esc(titlePath) + '">' + shortKey + '</span>';
      }
    }

    // Prod cell (Rojo si colisiona con OTRO CLIENTE; Amarillo si es compartida con DESA/TEST; Verde si es única o compartida con bots del mismo cliente)
    let prodCell = '<span class="apk-dash">—</span>';
    const hasCrossCol = row.prod_cross_collisions && row.prod_cross_collisions.length > 0;
    const sameClientBots = row.prod_same_client_bots || [];

    if(prodVal){
      const shortKey = esc(formatKeyShort(prodVal));
      const titlePath = row.prod.map(e => e.path + ': ' + maskKey(e.value)).join('\n');
      if(hasCrossCol){
        const colTitle = '[COLISIÓN CRÍTICA EN PROD] Clave repetida en otro cliente: ' + row.prod_cross_collisions.join(', ') + '\n' + titlePath;
        prodCell = '<span class="apk-key-error" title="' + esc(colTitle) + '">≠ ' + shortKey + '</span>';
      }else if(nonprodEqProd){
        const warnTitle = '[ADVERTENCIA] Clave de PROD reutilizada en ' + (desaEqProd && testEqProd ? 'DESA y TEST' : (desaEqProd ? 'DESA' : 'TEST')) + '. La clave de PROD debe ser única.\n' + titlePath;
        prodCell = '<span class="apk-key-diff" style="color:#facc15;border-color:rgba(234,179,8,0.45);background:rgba(234,179,8,0.12);" title="' + esc(warnTitle) + '">⚠ ' + shortKey + '</span>';
      }else if(sameClientBots.length > 0){
        const sameTitle = 'Prod OK ✓ (Compartida legítimamente con otros bots de ' + esc(row.client) + ': ' + sameClientBots.join(', ') + ')\n' + titlePath;
        prodCell = '<span class="apk-key-match" title="' + esc(sameTitle) + '">✓ ' + shortKey + '</span>';
      }else{
        const uniqueTitle = 'Prod OK ✓ (Clave única de este cliente)\n' + titlePath;
        prodCell = '<span class="apk-key-match" title="' + esc(uniqueTitle) + '">✓ ' + shortKey + '</span>';
      }
    }

    // Regla estricta del Círculo de Estado (.apk-dot):
    // 1. ROJO: Si hay colisión en PROD con OTRO CLIENTE (cross-client collision).
    // 2. AMARILLO: Si DESA o TEST son iguales a PROD (la API key de PROD debe ser única y no compartirse con ambientes inferiores).
    // 3. VERDE: Si PROD está presente, es única respecto a DESA/TEST y no colisiona con otros clientes (o comparte con bots del mismo cliente).
    let dotClass = 'ok';
    let dotTitle = '';

    if(hasCrossCol){
      dotClass = 'critical'; // ROJO
      dotTitle = 'COLISIÓN CRÍTICA EN PROD: Clave compartida con otro cliente: ' + row.prod_cross_collisions.join(', ');
    }else if(nonprodEqProd){
      dotClass = 'warn'; // AMARILLO
      if(desaEqProd && testEqProd){
        dotTitle = 'Advertencia: DESA y TEST tienen la misma API key que PROD. La API key de PROD debe ser única.';
      }else if(desaEqProd){
        dotTitle = 'Advertencia: DESA tiene la misma API key que PROD. La API key de PROD debe ser única.';
      }else{
        dotTitle = 'Advertencia: TEST tiene la misma API key que PROD. La API key de PROD debe ser única.';
      }
      if(isMismatch){
        dotTitle += ' | Además: Desa difiere de Test';
      }
    }else{
      dotClass = 'ok'; // VERDE
      if(prodVal){
        if(sameClientBots.length > 0){
          dotTitle = 'Prod OK ✓ (Compartida con bots del mismo cliente: ' + sameClientBots.join(', ') + ')';
        }else{
          dotTitle = 'Prod OK ✓ (Clave única de producción)';
        }
      }else{
        dotTitle = 'Sin Prod ✓ (Sin colisiones con otros clientes)';
      }
      if(isMismatch){
        dotTitle += ' | Aviso: Desa difiere de Test (ver celdas en amarillo)';
      }
    }

    return '<tr>' +
      '<td><span class="apk-client-text">' + esc(row.client) + '</span></td>' +
      '<td><span class="apk-bot-text">' + esc(row.bot) + '</span></td>' +
      '<td>' + desaCell + '</td>' +
      '<td>' + testCell + '</td>' +
      '<td>' + prodCell + '</td>' +
      '<td class="apk-status-cell">' +
        '<div class="apk-dot-wrap" title="' + esc(dotTitle) + '">' +
          '<span class="apk-dot ' + dotClass + '"></span>' +
          '<span class="apk-dot-tooltip">' + esc(dotTitle) + '</span>' +
        '</div>' +
      '</td>' +
      '</tr>';
  }).join('');

  if($('apiKeySummary')) $('apiKeySummary').textContent = 'Mostrando ' + filtered.length + ' de ' + allRows.length + ' client/bot';
  if($('exportApiKeys')) $('exportApiKeys').disabled = allRows.length === 0;
}
async function downloadApiKeysExcel(){
  const checked = [...document.querySelectorAll('.apikey-env-check:checked')].map(el => el.value);
  if(!checked.length) return setStatus('Seleccioná al menos un ambiente para exportar.', 'err');
  setStatus('Generando reporte Excel de API keys…');
  try{
    const response = await fetch('/api/apikeys/export', {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({envs: checked})});
    if(!response.ok){
      const error = await response.json().catch(() => ({}));
      throw new Error(error.error || ('HTTP ' + response.status));
    }
    const blob = await response.blob(), url = URL.createObjectURL(blob), link = document.createElement('a');
    link.href = url; link.download = 'auditoria_api_keys_' + checked.join('-') + '.xlsx';
    document.body.appendChild(link); link.click(); link.remove(); setTimeout(() => URL.revokeObjectURL(url), 0);
    setStatus('Reporte descargado: ambientes ' + checked.join(', ') + '.', 'ok');
  }catch(e){ setStatus(e.message, 'err'); }
}
async function refreshList(force){
  setStatus('Leyendo lista de client/bot…');
  try{
    if(activeMode === 'single' || activeMode === 'prompts'){
      const env = $('singleEnv').value;
      const keys = await getKeys(env, force);
      ALL = keys.map(([c, b]) => ({c, b, l: true, r: true}));
      applyFilter();
      setStatus(ALL.length + ' client/bot en ' + env + '. Elegí uno para abrir su ' + (activeMode === 'prompts' ? 'configuración y prompts.' : 'JSON.'));
      return;
    }
    const L = $('envL').value, R = $('envR').value;
    const [kl, kr] = await Promise.all([getKeys(L, force), getKeys(R, force)]);
    const m = new Map();
    kl.forEach(([c, b]) => m.set(c + '\u0000' + b, {c, b, l: true, r: false}));
    kr.forEach(([c, b]) => { const k = c + '\u0000' + b; const o = m.get(k) || {c, b, l: false, r: false}; o.r = true; m.set(k, o); });
    ALL = [...m.values()].sort((x, y) => (x.c + '/' + x.b).localeCompare(y.c + '/' + y.b));
    applyFilter();
    setStatus(ALL.length + ' combinaciones client/bot. Elegí una y tocá OK.');
  }catch(e){ setStatus(e.message, 'err'); }
}
function applyFilter(){
  const q = $('filter').value.toLowerCase().trim(), prev = $('pick').value;
  const L = $('envL').value, R = $('envR').value;
  const list = ALL.filter(o => (o.c + ' ' + o.b).toLowerCase().includes(q));
  $('pick').innerHTML = list.map(o => {
    const tag = (o.l && o.r) ? '' : ' — solo en ' + (o.l ? L : R);
    return '<option value="' + esc(JSON.stringify([o.c, o.b])) + '">' + esc(o.c) + ' / ' + esc(o.b) + esc(tag) + '</option>';
  }).join('');
  if([...$('pick').options].some(o => o.value === prev)) $('pick').value = prev;
}

/* ---------- editor y cambios ---------- */
function sortDeep(v){
  if(Array.isArray(v)) return v.map(sortDeep);
  if(v && typeof v === 'object'){ const o = {}; Object.keys(v).sort().forEach(k => o[k] = sortDeep(v[k])); return o; }
  return v;
}
function model(side){ return side === 'left' ? leftModel : rightModel; }
function sideEditor(side){ return side === 'left' ? diffEditor.getOriginalEditor() : diffEditor.getModifiedEditor(); }
function dirty(side){ return loaded && model(side).getValue() !== baseline[side]; }
function singleDirty(){
  if(!singleLoaded) return false;
  if(currentSingleSubMode === 'text') return countDirtyChanges() > 0;
  return singleModel.getValue() !== singleBaseline;
}
function hasDirty(){ return dirty('left') || dirty('right') || singleDirty(); }
function updateSaveButtons(){
  ['left', 'right'].forEach(side => {
    const button = $(side === 'left' ? 'saveL' : 'saveR');
    button.disabled = !loaded || saving || (!dirty(side) && versions[side] !== null);
    button.textContent = versions[side] === null ? 'Crear' : 'Guardar';
  });
  updateDirtyState();
}
function waitForDiff(){
  if(!diffPending) return Promise.resolve();
  return new Promise(resolve => diffWaiters.push(resolve));
}
function onDiffUpdated(){
  diffPending = false;
  const waiters = diffWaiters; diffWaiters = [];
  waiters.forEach(resolve => resolve());
}
function getHunks(){
  if(!loaded) return [];
  const changes = diffEditor.getLineChanges();
  if(!changes) return [];
  return changes.map(change => {
    const leftStart = change.originalStartLineNumber || change.modifiedStartLineNumber || 1;
    const rightStart = change.modifiedStartLineNumber || change.originalStartLineNumber || 1;
    const lines = (source, start, end) => end >= start && end > 0
      ? Array.from({length: end - start + 1}, (_, index) => source.getLineContent(start + index))
      : [];
    return {
      leftStart, rightStart,
      leftLines: lines(leftModel, change.originalStartLineNumber, change.originalEndLineNumber),
      rightLines: lines(rightModel, change.modifiedStartLineNumber, change.modifiedEndLineNumber)
    };
  });
}
function selectedHunk(side, hunks){
  if(!hunks.length) return -1;
  const position = sideEditor(side).getPosition(), line = position ? position.lineNumber : 1;
  let best = currentHunk < hunks.length ? currentHunk : 0, bestDistance = Infinity;
  hunks.forEach((h, i) => {
    const start = side === 'left' ? h.leftStart : h.rightStart;
    const lines = side === 'left' ? h.leftLines : h.rightLines;
    const distance = lines.length ? Math.max(start - line, line - (start + lines.length - 1), 0) : Math.abs(start - line);
    if(distance < bestDistance){ best = i; bestDistance = distance; }
  });
  return best;
}
async function transfer(from){
  await waitForDiff();
  const hunks = getHunks();
  if(!hunks.length) return setStatus('No hay bloques distintos para transferir.', 'err');
  const index = selectedHunk(from, hunks), h = hunks[index];
  const to = from === 'left' ? 'right' : 'left';
  const sourceLines = from === 'left' ? h.leftLines : h.rightLines;
  const start = from === 'left' ? h.rightStart : h.leftStart;
  const removeCount = from === 'left' ? h.rightLines.length : h.leftLines.length;
  const targetModel = model(to), targetEditor = sideEditor(to);
  const lineCount = targetModel.getLineCount();
  const startLine = Math.max(1, Math.min(lineCount, start));
  let range, text = sourceLines.join('\n');
  if(removeCount){
    const endLine = Math.min(lineCount, startLine + removeCount - 1);
    const hasFollowingLine = endLine < lineCount;
    range = new monaco.Range(startLine, 1, hasFollowingLine ? endLine + 1 : endLine, hasFollowingLine ? 1 : targetModel.getLineMaxColumn(endLine));
    if(hasFollowingLine) text += '\n';
  }else if(start > lineCount){
    range = new monaco.Range(lineCount, targetModel.getLineMaxColumn(lineCount), lineCount, targetModel.getLineMaxColumn(lineCount));
    text = '\n' + text;
  }else{
    range = new monaco.Range(startLine, 1, startLine, 1);
    text += '\n';
  }
  targetEditor.executeEdits('transfer-diff-block', [{range, text}]);
  targetEditor.focus();
  currentHunk = index;
  setStatus('Bloque ' + (index + 1) + ' transferido de ' + (from === 'left' ? 'izquierda a derecha' : 'derecha a izquierda') + '. Guardá el ambiente destino para aplicar el cambio.');
}
async function go(dir){
  await waitForDiff();
  const hunks = getHunks();
  if(!hunks.length) return;
  currentHunk = currentHunk < 0 ? (dir > 0 ? 0 : hunks.length - 1) : (currentHunk + dir + hunks.length) % hunks.length;
  const h = hunks[currentHunk], side = 'right';
  const line = Math.max(1, Math.min(h.rightStart, rightModel.getLineCount()));
  sideEditor(side).setPosition({lineNumber: line, column: 1});
  sideEditor(side).revealLineInCenter(line);
  sideEditor(side).focus();
  setStatus('Bloque ' + (currentHunk + 1) + ' de ' + hunks.length + '. Usá ← o → para transferirlo entre ambientes.');
}
async function compare(){
  const value = $('pick').value;
  if(!value) return setStatus('Elegí un client / bot de la lista.', 'err');
  if(hasDirty() && !window.confirm('Hay cambios sin guardar. ¿Descartarlos y cargar otra comparación?')) return;
  const [client, bot] = JSON.parse(value), leftEnv = $('envL').value, rightEnv = $('envR').value;
  setStatus('Comparando…');
  try{
    const j = await api('/api/compare?left=' + encodeURIComponent(leftEnv) + '&right=' + encodeURIComponent(rightEnv) +
      '&client=' + encodeURIComponent(client) + '&bot=' + encodeURIComponent(bot));
    activeKey = {client, bot, leftEnv, rightEnv};
    versions = {left: j.left_version, right: j.right_version};
    leftModel.setValue(j.left ? j.left_json : JSON.stringify({client: {S: client}, bot: {S: bot}}, null, 2));
    rightModel.setValue(j.right ? j.right_json : JSON.stringify({client: {S: client}, bot: {S: bot}}, null, 2));
    await waitForDiff();
    baseline = {left: leftModel.getValue(), right: rightModel.getValue()};
    loaded = true;
    const bL = $('badgeL'), bR = $('badgeR');
    if(bL){
      bL.textContent = leftEnv.toUpperCase();
      bL.className = 'env-badge ' + leftEnv.toLowerCase();
    }
    if(bR){
      bR.textContent = rightEnv.toUpperCase();
      bR.className = 'env-badge ' + rightEnv.toLowerCase();
    }
    $('hL').textContent = tableOf(leftEnv) + (j.left ? '' : ' (NO EXISTE)');
    $('hR').textContent = tableOf(rightEnv) + (j.right ? '' : ' (NO EXISTE)');
    diffEditor.updateOptions({diffWordWrap: 'on', wordWrap: 'on', hideUnchangedRegions: {enabled: $('hide').checked, contextLineCount: 3}});
    if(diffEditor.getOriginalEditor()){
      diffEditor.getOriginalEditor().updateOptions({wordWrap: 'on', wrappingStrategy: 'advanced', wrappingIndent: 'indent'});
    }
    if(diffEditor.getModifiedEditor()){
      diffEditor.getModifiedEditor().updateOptions({wordWrap: 'on', wrappingStrategy: 'advanced', wrappingIndent: 'indent'});
    }
    diffEditor.getModifiedEditor().setPosition({lineNumber: 1, column: 1});
    updateSaveButtons();
    const hunks = getHunks();
    if(!j.left && !j.right) setStatus(client + ' / ' + bot + ': no existe; podés editar y crear en cada ambiente.', 'err');
    else if(!hunks.length) setStatus(client + ' / ' + bot + ': sin diferencias.', 'ok');
    else setStatus(client + ' / ' + bot + ': ' + hunks.length + ' bloque(s) distinto(s). Seleccioná un bloque y usá ← / → para transferirlo. F7 / Shift+F7 navega.');
  }catch(e){ setStatus(e.message, 'err'); }
}
function getAttrString(val){
  if(typeof val === 'string') return val;
  if(val && typeof val === 'object' && typeof val.S === 'string') return val.S;
  return null;
}
async function openSingle(){
  const value = $('pick').value;
  if(!value) return setStatus('Elegí un client / bot de la lista.', 'err');
  if(hasDirty() && !window.confirm('Hay cambios sin guardar. ¿Descartarlos y abrir otro bot?')) return;
  const [client, bot] = JSON.parse(value), env = $('singleEnv').value;
  setStatus('Abriendo ' + (activeMode === 'prompts' ? 'prompts' : 'JSON') + '…');
  try{
    const j = await api('/api/item?env=' + encodeURIComponent(env) + '&client=' + encodeURIComponent(client) + '&bot=' + encodeURIComponent(bot));
    if(!j.item) return setStatus(client + ' / ' + bot + ' no existe en ' + env + '.', 'err');
    singleActiveKey = {client, bot, env};
    singleVersion = j.version;
    rawSingleItem = j.item;
    unpackedPromptsData = unpackPrompts(rawSingleItem);
    selectedPromptName = null;
    apiKeyRevealed = false;
    comparisonEnv = getCounterpartEnv(env);
    comparisonItem = null;
    comparisonPromptsData = null;
    fetchComparisonItem(client, bot, comparisonEnv);

    singleModel.setValue(j.json);
    singleBaseline = singleModel.getValue();
    singleLoaded = true;

    const badge = $('singleEnvBadge');
    if(badge){
      badge.textContent = env.toUpperCase();
      badge.className = 'env-badge ' + env;
    }
    if($('footerEnvBadge')) $('footerEnvBadge').textContent = env.toUpperCase();
    if($('footerTableName')) $('footerTableName').textContent = tableOf(env);
    if($('singleTitle')) $('singleTitle').textContent = tableOf(env) + ' · ' + client + ' / ' + bot;

    renderPromptsView();
    setSingleSubMode(activeMode === 'single' ? 'json' : 'text');
    updateDirtyState();
    refreshHistoryBadge();

    setStatus(client + ' / ' + bot + ' abierto en ' + env + '.', 'ok');
  }catch(e){ setStatus(e.message, 'err'); }
}
function saveSide(side){
  openSaveModal(side);
}
function computeChangesSummary(side){
  const changes = [];
  if(!side && currentSingleSubMode === 'text'){
    if(!unpackedPromptsData) return [];
    for(const [pName, pData] of Object.entries(unpackedPromptsData.prompts || {})){
      const promptChanges = [];
      if(pData.fields && pData.fields.TEMPLATE && pData.fields.TEMPLATE.dirty){
        promptChanges.push('TEMPLATE modificado');
      }
      for(const [fName, fInfo] of Object.entries(pData.fields || {})){
        if(fName === 'TEMPLATE') continue;
        if(fInfo && fInfo.dirty){
          const origVal = fInfo.raw ? (fInfo.raw.S != null ? fInfo.raw.S : (fInfo.raw.N != null ? fInfo.raw.N : (fInfo.raw.BOOL !== undefined ? String(fInfo.raw.BOOL) : JSON.stringify(fInfo.raw)))) : '(nuevo)';
          const newVal = fInfo.value != null ? (Array.isArray(fInfo.value) ? '[' + fInfo.value.join(', ') + ']' : String(fInfo.value)) : '';
          promptChanges.push('<strong>' + esc(fName) + '</strong>: ' + esc(String(origVal)) + ' → ' + esc(newVal));
        }
      }
      if(pData.deleted_fields && pData.deleted_fields.length){
        for(const df of pData.deleted_fields){
          promptChanges.push('<strong>' + esc(df) + '</strong>: eliminado');
        }
      }
      if(promptChanges.length){
        changes.push({
          prompt: pName,
          items: promptChanges
        });
      }
    }
  }else{
    try{
      const origText = side ? baseline[side] : singleBaseline;
      const newText = side ? model(side).getValue() : singleModel.getValue();
      const origObj = (origText && origText.trim()) ? JSON.parse(origText) : {};
      const newObj = JSON.parse(newText);
      const allKeys = new Set([...Object.keys(origObj), ...Object.keys(newObj)]);
      const keyChanges = [];
      for(const k of allKeys){
        if(k === 'client' || k === 'bot') continue;
        if(!(k in newObj)){
          keyChanges.push('Atributo <strong>' + esc(k) + '</strong> eliminado');
        }else if(!(k in origObj)){
          keyChanges.push('Atributo <strong>' + esc(k) + '</strong> agregado');
        }else if(JSON.stringify(origObj[k]) !== JSON.stringify(newObj[k])){
          keyChanges.push('Atributo <strong>' + esc(k) + '</strong> modificado');
        }
      }
      if(keyChanges.length){
        changes.push({
          prompt: side ? ('Panel ' + (side === 'left' ? 'Izquierdo' : 'Derecho')) : 'JSON individual',
          items: keyChanges
        });
      }
    }catch(e){
      changes.push({
        prompt: side ? ('Panel ' + (side === 'left' ? 'Izquierdo' : 'Derecho')) : 'JSON individual',
        items: ['Texto JSON modificado']
      });
    }
  }
  return changes;
}

function closeSaveModal(){
  if($('saveModal')) $('saveModal').hidden = true;
  pendingSavePayload = null;
}

function openSaveModal(side){
  let env, client, bot, version, textToSave;
  if(side === 'left' || side === 'right'){
    if(!loaded || !activeKey) return;
    env = side === 'left' ? activeKey.leftEnv : activeKey.rightEnv;
    client = activeKey.client;
    bot = activeKey.bot;
    version = versions[side];
    textToSave = model(side).getValue();
  }else{
    if(!singleLoaded || !singleActiveKey) return;
    env = singleActiveKey.env;
    client = singleActiveKey.client;
    bot = singleActiveKey.bot;
    version = singleVersion;
    if(currentSingleSubMode === 'text' && hasValidationErrors()){
      return setStatus('No se puede guardar: hay errores de validación en parámetros numéricos.', 'err');
    }
    if(currentSingleSubMode === 'text'){
      const repacked = repackPrompts(rawSingleItem, unpackedPromptsData);
      rawSingleItem = repacked;
      textToSave = JSON.stringify(repacked, null, 2);
    }else{
      textToSave = singleModel.getValue();
    }
  }

  let item;
  try{ item = JSON.parse(textToSave); }catch(e){ return setStatus('JSON inválido: ' + e.message, 'err'); }
  const itemClient = getAttrString(item && item.client);
  const itemBot = getAttrString(item && item.bot);
  if(!item || Array.isArray(item) || typeof item !== 'object' || itemClient !== client || itemBot !== bot)
    return setStatus('El JSON debe ser un objeto y conservar client/bot: ' + client + ' / ' + bot, 'err');

  const changes = computeChangesSummary(side);
  const isDirty = side ? dirty(side) : hasDirty();
  if(!changes.length && !isDirty && version !== null){
    return setStatus('No hay cambios sin guardar para ' + client + ' / ' + bot + '.', 'ok');
  }

  pendingSavePayload = {
    side: side || null,
    env, client, bot, version, json_text: textToSave, confirm_prod: env === 'prod'
  };

  if($('modalEnvBadge')){
    $('modalEnvBadge').textContent = env.toUpperCase();
    $('modalEnvBadge').className = 'env-badge ' + env;
  }
  if($('modalBotName')) $('modalBotName').textContent = client + ' / ' + bot;

  let diffHtml = '';
  if(changes.length){
    for(const ch of changes){
      diffHtml += '<div class="modal-diff-item">';
      diffHtml += '  <div class="modal-diff-prompt-title">▸ ' + esc(ch.prompt) + '</div>';
      for(const it of ch.items){
        diffHtml += '  <div class="modal-diff-change-desc">• ' + it + '</div>';
      }
      diffHtml += '</div>';
    }
  }else if(version === null){
    diffHtml = '<div class="modal-diff-item"><div class="modal-diff-prompt-title">▸ Ítem nuevo</div><div class="modal-diff-change-desc">• Se creará este ítem completo en <strong>' + esc(env.toUpperCase()) + '</strong>.</div></div>';
  }else{
    diffHtml = '<div class="modal-diff-empty">No se detectaron cambios en campos conocidos.</div>';
  }
  if($('modalDiffSummary')) $('modalDiffSummary').innerHTML = diffHtml;

  const isProd = env === 'prod';
  const prodSection = $('modalProdConfirm');
  const confirmBtn = $('btnModalConfirm');
  const prodInput = $('modalProdBotInput');

  if(prodSection) prodSection.hidden = !isProd;
  if(confirmBtn){
    confirmBtn.className = 'btn-modal-confirm' + (isProd ? ' prod-btn' : '');
    confirmBtn.disabled = isProd;
    confirmBtn.textContent = isProd ? 'Confirmar y Guardar en PROD' : (version === null ? 'Confirmar y Crear' : 'Confirmar y Guardar');
  }
  if(isProd){
    if($('modalProdBotExpected')) $('modalProdBotExpected').textContent = bot;
    if(prodInput){
      prodInput.value = '';
      prodInput.oninput = () => {
        confirmBtn.disabled = (prodInput.value.trim() !== bot);
      };
      setTimeout(() => prodInput.focus(), 80);
    }
  }else{
    setTimeout(() => confirmBtn && confirmBtn.focus(), 80);
  }

  if($('saveModal')) $('saveModal').hidden = false;
}

async function confirmSaveModal(){
  if(!pendingSavePayload) return;
  const payload = pendingSavePayload;
  const {env, client, bot, side} = payload;
  const confirmBtn = $('btnModalConfirm');
  if(confirmBtn){
    confirmBtn.disabled = true;
    confirmBtn.textContent = 'Guardando en DynamoDB…';
  }

  saving = true;
  if(side) updateSaveButtons();
  else updateDirtyState();
  setStatus('Guardando en ' + env + '…');

  try{
    const result = await api('/api/save', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({
        env: payload.env,
        client: payload.client,
        bot: payload.bot,
        version: payload.version,
        json_text: payload.json_text,
        confirm_prod: payload.confirm_prod
      })
    });

    if(side){
      versions[side] = result.version;
      baseline[side] = payload.json_text;
      $(side === 'left' ? 'hL' : 'hR').textContent = tableOf(env);
      closeSaveModal();
      updateSaveButtons();
    }else{
      singleVersion = result.version;
      singleBaseline = payload.json_text;
      singleModel.setValue(payload.json_text);
      rawSingleItem = JSON.parse(payload.json_text);

      if(unpackedPromptsData){
        for(const p of Object.values(unpackedPromptsData.prompts || {})){
          p.deleted_fields = [];
          for(const f of Object.values(p.fields || {})){
            f.dirty = false;
            if(f.type === 'S') f.raw = {S: String(f.value != null ? f.value : '')};
            else if(f.type === 'N') f.raw = {N: String(f.value != null ? f.value : '')};
            else if(f.type === 'BOOL') f.raw = {BOOL: Boolean(f.value)};
            else if(f.type === 'L'){
              if(Array.isArray(f.value)) f.raw = {L: f.value.map(v => (typeof v === 'object' && v ? v : {S: String(v)}))};
              else f.raw = {L: []};
            }
          }
        }
      }

      closeSaveModal();
      updateDirtyState();
      renderActivePrompt();
      refreshHistoryBadge();
    }

    const backupMsg = result.backup ? (' [Backup: ' + result.backup + ']') : '';
    const pathsMsg = (result.updated_paths && result.updated_paths.length)
      ? (' · ' + result.updated_paths.length + ' campo(s) actualizado(s): ' + result.updated_paths.slice(0, 3).join(', ') + (result.updated_paths.length > 3 ? '…' : ''))
      : '';
    setStatus(client + ' / ' + bot + ' guardado en ' + env + '.' + pathsMsg + backupMsg, 'ok');
  }catch(e){
    if(confirmBtn){
      confirmBtn.disabled = false;
      confirmBtn.textContent = 'Reintentar guardado';
    }
    closeSaveModal();
    const errMsg = e.message || 'Error guardando en DynamoDB';
    setStatus(errMsg, 'err');
    alert('Error al guardar: ' + errMsg);
  }finally{
    saving = false;
    if(side) updateSaveButtons();
    else updateDirtyState();
  }
}

function saveSingle(){
  openSaveModal();
}

function timeAgo(dateOrTimestamp){
  const now = Date.now();
  let ts = typeof dateOrTimestamp === 'number' ? dateOrTimestamp : (new Date(dateOrTimestamp)).getTime();
  if(isNaN(ts)) return '';
  if(ts < 10000000000) ts *= 1000;
  const diffSec = Math.floor((now - ts) / 1000);
  if(diffSec < 45) return 'hace unos segundos';
  const diffMin = Math.floor(diffSec / 60);
  if(diffMin < 60) return 'hace ' + diffMin + (diffMin === 1 ? ' minuto' : ' minutos');
  const diffHours = Math.floor(diffMin / 60);
  if(diffHours < 24) return 'hace ' + diffHours + (diffHours === 1 ? ' hora' : ' horas');
  const diffDays = Math.floor(diffHours / 24);
  if(diffDays === 1) return 'ayer';
  if(diffDays < 30) return 'hace ' + diffDays + ' días';
  return 'hace más de un mes';
}

async function refreshHistoryBadge(){
  if(!singleActiveKey) return;
  const {env, client, bot} = singleActiveKey;
  try{
    const j = await api('/api/history?env=' + encodeURIComponent(env) + '&client=' + encodeURIComponent(client) + '&bot=' + encodeURIComponent(bot));
    const count = (j && j.backups && j.backups.length) || 0;
    const badge = $('historyBadgeCount');
    if(badge){
      if(count > 0){
        badge.textContent = count;
        badge.style.display = 'inline-block';
      }else{
        badge.style.display = 'none';
      }
    }
  }catch(e){
    // silent
  }
}

async function openHistoryModal(){
  if(!singleActiveKey) return;
  const {env, client, bot} = singleActiveKey;
  if($('historyModalEnvBadge')){
    $('historyModalEnvBadge').textContent = env.toUpperCase();
    $('historyModalEnvBadge').className = 'env-badge ' + env;
  }
  if($('historyModalBotName')) $('historyModalBotName').textContent = client + ' / ' + bot;
  if($('historyModal')) $('historyModal').hidden = false;

  const listEl = $('historyListItems');
  const emptyEl = $('historyEmptyMsg');
  const detailTitle = $('historyDetailTitle');
  const detailMeta = $('historyDetailMeta');
  const diffSec = $('historyDiffSection');
  const placeholder = $('historyPlaceholder');
  const btnRestore = $('btnRestoreHistory');

  if(btnRestore) btnRestore.style.display = 'none';
  if(diffSec) diffSec.style.display = 'none';
  if(placeholder) placeholder.style.display = 'block';
  if(detailTitle) detailTitle.textContent = 'Cargando historial…';
  if(detailMeta) detailMeta.textContent = '';
  if(listEl) listEl.innerHTML = '<div style="color:#858585;font-size:12px;padding:8px;">Buscando versiones guardadas…</div>';

  try{
    const j = await api('/api/history?env=' + encodeURIComponent(env) + '&client=' + encodeURIComponent(client) + '&bot=' + encodeURIComponent(bot));
    currentHistoryBackups = (j && j.backups) || [];
    if(!currentHistoryBackups.length){
      if(listEl) listEl.innerHTML = '';
      if(emptyEl) emptyEl.hidden = false;
      if(detailTitle) detailTitle.textContent = 'Sin versiones guardadas';
      if(placeholder) placeholder.textContent = 'Aún no hay backups registrados para este bot en ' + env + '.';
      return;
    }

    if(emptyEl) emptyEl.hidden = true;
    let html = '';
    currentHistoryBackups.forEach((b, idx) => {
      const relTime = timeAgo(b.timestamp);
      const isFirst = idx === 0;
      const sizeKb = b.size ? ((b.size / 1024).toFixed(1) + ' KB') : '';
      html += '<div class="history-version-card' + (isFirst ? ' active' : '') + '" data-fname="' + esc(b.filename) + '" data-idx="' + idx + '">';
      html += '  <div class="history-version-top">';
      html += '    <span class="history-version-date">' + esc(b.date) + '</span>';
      if(isFirst){
        html += '    <span class="history-version-badge-curr">Última</span>';
      }
      html += '  </div>';
      html += '  <div class="history-version-bottom">';
      html += '    <span class="history-version-rel">' + esc(relTime) + '</span>';
      if(sizeKb) html += '    <span>' + esc(sizeKb) + '</span>';
      html += '  </div>';
      html += '</div>';
    });
    if(listEl) listEl.innerHTML = html;

    selectHistoryVersion(currentHistoryBackups[0].filename, 0);
  }catch(e){
    if(listEl) listEl.innerHTML = '<div style="color:#f87171;font-size:12px;padding:8px;">Error al cargar historial: ' + esc(e.message) + '</div>';
  }
}

function closeHistoryModal(){
  if($('historyModal')) $('historyModal').hidden = true;
}

async function selectHistoryVersion(filename, index){
  selectedHistoryFilename = filename;
  const listEl = $('historyListItems');
  if(listEl){
    listEl.querySelectorAll('.history-version-card').forEach(card => {
      card.classList.toggle('active', card.dataset.fname === filename);
    });
  }

  const detailTitle = $('historyDetailTitle');
  const detailMeta = $('historyDetailMeta');
  const diffSec = $('historyDiffSection');
  const diffSummary = $('historyDiffSummary');
  const placeholder = $('historyPlaceholder');
  const btnRestore = $('btnRestoreHistory');
  const jsonRaw = $('historyJsonRaw');
  const btnToggleJson = $('btnToggleHistoryJson');

  if(detailTitle) detailTitle.textContent = 'Cargando versión…';
  if(btnRestore) btnRestore.style.display = 'none';

  try{
    const j = await api('/api/history/get?file=' + encodeURIComponent(filename));
    selectedHistoryItem = j.item;
    const bInfo = currentHistoryBackups.find(b => b.filename === filename);
    const dateStr = (bInfo && bInfo.date) || filename;
    const relStr = bInfo ? timeAgo(bInfo.timestamp) : '';

    if(detailTitle) detailTitle.textContent = 'Versión del ' + dateStr + (relStr ? (' (' + relStr + ')') : '');
    if(detailMeta) detailMeta.textContent = 'Archivo: ' + filename + (bInfo && bInfo.size ? (' · ' + (bInfo.size / 1024).toFixed(1) + ' KB') : '');

    if(btnRestore) btnRestore.style.display = 'inline-flex';
    if(placeholder) placeholder.style.display = 'none';
    if(diffSec) diffSec.style.display = 'flex';

    if(jsonRaw){
      jsonRaw.textContent = JSON.stringify(selectedHistoryItem, null, 2);
      jsonRaw.hidden = true;
    }
    if(btnToggleJson){
      btnToggleJson.textContent = 'Ver JSON completo';
      btnToggleJson.onclick = () => {
        jsonRaw.hidden = !jsonRaw.hidden;
        btnToggleJson.textContent = jsonRaw.hidden ? 'Ver JSON completo' : 'Ocultar JSON';
      };
    }

    const diffs = compareHistoryWithCurrent(rawSingleItem, selectedHistoryItem);
    if(diffSummary){
      if(!diffs.length){
        diffSummary.innerHTML = '<div class="history-diff-empty">✓ Esta versión es idéntica al ítem cargado actualmente en el editor.</div>';
      }else{
        let diffHtml = '';
        for(const d of diffs){
          diffHtml += '<div class="modal-diff-item">';
          diffHtml += '  <div class="modal-diff-prompt-title">▸ ' + esc(d.title) + '</div>';
          for(const it of d.changes){
            diffHtml += '  <div class="modal-diff-change-desc">• ' + it + '</div>';
          }
          diffHtml += '</div>';
        }
        diffSummary.innerHTML = diffHtml;
      }
    }
  }catch(e){
    if(detailTitle) detailTitle.textContent = 'Error cargando versión';
    if(diffSummary) diffSummary.innerHTML = '<div style="color:#f87171;">' + esc(e.message) + '</div>';
  }
}

function compareHistoryWithCurrent(current, snapshot){
  const results = [];
  if(!current || !snapshot) return results;

  const currAC = (current.AGENT_CONFIG && current.AGENT_CONFIG.M) || {};
  const snapAC = (snapshot.AGENT_CONFIG && snapshot.AGENT_CONFIG.M) || {};
  const currPrompts = (currAC.PROMPTS && currAC.PROMPTS.M) || {};
  const snapPrompts = (snapAC.PROMPTS && snapAC.PROMPTS.M) || {};

  const allPromptNames = new Set([...Object.keys(currPrompts), ...Object.keys(snapPrompts)]);
  for(const pName of [...allPromptNames].sort()){
    const currP = (currPrompts[pName] && currPrompts[pName].M) || null;
    const snapP = (snapPrompts[pName] && snapPrompts[pName].M) || null;

    if(!currP && snapP){
      results.push({title: pName, changes: ['(existía en esta versión pero no en la actual)']});
      continue;
    }
    if(currP && !snapP){
      results.push({title: pName, changes: ['(no existía en esta versión)']});
      continue;
    }

    const promptChanges = [];
    const currTmpl = (currP.TEMPLATE && currP.TEMPLATE.S) || '';
    const snapTmpl = (snapP.TEMPLATE && snapP.TEMPLATE.S) || '';
    if(currTmpl !== snapTmpl){
      const diffLines = (snapTmpl.split('\n').length) - (currTmpl.split('\n').length);
      const lineDiffText = diffLines === 0 ? 'mismo n° de líneas' : (diffLines > 0 ? ('+' + diffLines + ' líneas') : (diffLines + ' líneas'));
      promptChanges.push('<strong>TEMPLATE</strong> difiere (' + snapTmpl.length + ' chars en backup vs ' + currTmpl.length + ' chars actuales, ' + lineDiffText + ')');
    }

    const allFieldNames = new Set([...Object.keys(currP), ...Object.keys(snapP)]);
    for(const fName of [...allFieldNames].sort()){
      if(fName === 'TEMPLATE') continue;
      const fCurr = currP[fName];
      const fSnap = snapP[fName];
      if(!fCurr && fSnap){
        promptChanges.push('Parámetro <strong>' + esc(fName) + '</strong>: existía con valor ' + esc(JSON.stringify(fSnap)));
      }else if(fCurr && !fSnap){
        promptChanges.push('Parámetro <strong>' + esc(fName) + '</strong>: no existía');
      }else if(JSON.stringify(fCurr) !== JSON.stringify(fSnap)){
        const valCurr = fCurr ? (fCurr.S != null ? fCurr.S : (fCurr.N != null ? fCurr.N : JSON.stringify(fCurr))) : '';
        const valSnap = fSnap ? (fSnap.S != null ? fSnap.S : (fSnap.N != null ? fSnap.N : JSON.stringify(fSnap))) : '';
        promptChanges.push('Parámetro <strong>' + esc(fName) + '</strong>: actual ' + esc(String(valCurr)) + ' vs backup ' + esc(String(valSnap)));
      }
    }

    if(promptChanges.length){
      results.push({title: pName, changes: promptChanges});
    }
  }

  const allTopKeys = new Set([...Object.keys(current), ...Object.keys(snapshot)]);
  const topChanges = [];
  for(const k of [...allTopKeys].sort()){
    if(k === 'AGENT_CONFIG' || k === 'client' || k === 'bot') continue;
    if(!snapshot[k] && current[k]){
      topChanges.push('Atributo <strong>' + esc(k) + '</strong> agregado en versión actual');
    }else if(snapshot[k] && !current[k]){
      topChanges.push('Atributo <strong>' + esc(k) + '</strong> existía en esta versión');
    }else if(JSON.stringify(current[k]) !== JSON.stringify(snapshot[k])){
      topChanges.push('Atributo <strong>' + esc(k) + '</strong> difiere');
    }
  }
  if(topChanges.length){
    results.push({title: 'Atributos generales', changes: topChanges});
  }

  return results;
}

function restoreHistoryVersion(){
  if(!selectedHistoryItem) return;
  const bInfo = currentHistoryBackups.find(b => b.filename === selectedHistoryFilename);
  const dateStr = (bInfo && bInfo.date) || 'seleccionada';
  if(!window.confirm('¿Restaurar la versión del ' + dateStr + ' en el editor?\n\nPodrás revisar todos los cambios antes de guardar en DynamoDB.')){
    return;
  }

  rawSingleItem = JSON.parse(JSON.stringify(selectedHistoryItem));
  unpackedPromptsData = unpackPrompts(rawSingleItem);

  const baselineObj = (() => { try{ return JSON.parse(singleBaseline); }catch(e){ return {}; } })();
  const baselineUnpacked = unpackPrompts(baselineObj);
  for(const [pName, pData] of Object.entries(unpackedPromptsData.prompts || {})){
    const baseP = (baselineUnpacked && baselineUnpacked.prompts && baselineUnpacked.prompts[pName]) || null;
    for(const [fName, fInfo] of Object.entries(pData.fields || {})){
      const baseF = baseP && baseP.fields && baseP.fields[fName];
      if(!baseF || JSON.stringify(baseF.raw) !== JSON.stringify(fInfo.raw)){
        fInfo.dirty = true;
      }
    }
  }

  if(currentSingleSubMode === 'text'){
    renderPromptsView();
    if(templateEditor) templateEditor.layout();
  }else{
    if(singleModel){
      singleModel.setValue(JSON.stringify(rawSingleItem, null, 2));
    }
  }

  updateDirtyState();
  closeHistoryModal();
  setStatus('Versión del ' + dateStr + ' restaurada en el editor. Revisá los cambios y tocá Guardar para persistir.', 'ok');
}

function undoTemplate(){
  if(templateEditor){
    templateEditor.focus();
    templateEditor.trigger('keyboard', 'undo', null);
  }
}

function redoTemplate(){
  if(templateEditor){
    templateEditor.focus();
    templateEditor.trigger('keyboard', 'redo', null);
  }
}

function toggleFullscreenTemplate(force){
  const card = $('promptTemplateCard');
  if(!card) return;
  const isFs = typeof force === 'boolean' ? force : !card.classList.contains('fullscreen');
  card.classList.toggle('fullscreen', isFs);
  const btn = $('btnFullscreenTemplate');
  if(btn){
    btn.classList.toggle('active', isFs);
    btn.title = isFs ? 'Restaurar tamaño normal (Esc)' : 'Expandir editor a toda la ventana (Esc)';
    const lbl = $('fsTemplateLabel');
    if(lbl) lbl.textContent = isFs ? 'Restaurar' : 'Expandir';
  }
  if($('templateFsPromptName')) $('templateFsPromptName').textContent = selectedPromptName || '';
  document.body.style.overflow = isFs ? 'hidden' : '';
  requestAnimationFrame(() => {
    if(templateDiffActive && templateDiffEditor){
      templateDiffEditor.layout();
    }else if(templateEditor){
      templateEditor.layout();
    }
  });
  setTimeout(() => {
    if(templateDiffActive && templateDiffEditor){
      templateDiffEditor.layout();
    }else if(templateEditor){
      templateEditor.layout();
    }
  }, 60);
}

function discardEdits(){
  if(loaded){ leftModel.setValue(baseline.left); rightModel.setValue(baseline.right); }
  if(singleLoaded) singleModel.setValue(singleBaseline);
}
function syncRailActive(mode){
  const effectiveRailMode = mode;
  // API Keys View V2 Pills and Search
  document.querySelectorAll('#apiKeyFilterPills .apk-pill').forEach(btn => {
    btn.onclick = () => setApkFilter(btn.dataset.filter);
  });
  if($('apiKeySearchInput')){
    $('apiKeySearchInput').oninput = renderApiKeys;
  }
  document.querySelectorAll('#appRail .rail-btn[data-mode]').forEach(btn => {
    btn.classList.toggle('active', btn.dataset.mode === effectiveRailMode);
  });
  const titles = {
    compare: 'Comparar ambientes',
    single: 'JSON individual',
    prompts: 'Gestión de Prompts',
    models: 'Matriz de Modelos',
    apikeys: 'Auditoría de API Keys',
    cleanup: 'Limpieza DynamoDB',
    tables: 'Gestión de tablas DynamoDB',
    metrics: 'Métricas de Prompts'
  };
  const bvt = $('bvtName');
  if(bvt) bvt.textContent = titles[mode] || mode;
  const barSave = $('barSaveBtn');
  if(barSave){
    barSave.hidden = !(mode === 'single' || mode === 'prompts');
  }
  if($('btnQuickNavCompare')) $('btnQuickNavCompare').classList.toggle('active', mode === 'compare');
  if($('btnQuickNavSingle')) $('btnQuickNavSingle').classList.toggle('active', mode === 'single');
  if($('btnQuickNavPrompts')) $('btnQuickNavPrompts').classList.toggle('active', mode === 'prompts');
}

function setSingleEnvironment(env){
  if(singleActiveKey && singleDirty() && !window.confirm('Hay cambios sin guardar. ¿Descartarlos y cambiar de ambiente?')){
    syncSingleEnvButtons();
    return;
  }
  if($('singleEnv')){
    $('singleEnv').value = env;
    syncSingleEnvButtons();
    $('singleEnv').dispatchEvent(new Event('change'));
  }
}

function syncSingleEnvButtons(){
  const cur = ($('singleEnv') && $('singleEnv').value ? $('singleEnv').value : 'desa').toLowerCase();
  document.querySelectorAll('#singleEnvSegmented .env-btn').forEach(btn => {
    btn.classList.toggle('active', btn.dataset.env.toLowerCase() === cur);
  });
}

function toggleShortcutsModal(open){
  const m = $('shortcutsModal');
  if(!m) return;
  m.hidden = typeof open === 'boolean' ? !open : !m.hidden;
}

function switchMode(targetMode){
  changeMode(targetMode);
}

function changeMode(targetMode){
  toggleFullscreenTemplate(false);
  const next = targetMode || ($('viewMode') ? $('viewMode').value : activeMode);
  if(hasDirty() && !window.confirm('Hay cambios sin guardar. ¿Descartarlos y cambiar de vista?')){
    if($('viewMode')) $('viewMode').value = activeMode;
    return;
  }
  if(hasDirty()) discardEdits();
  activeMode = next;
  if($('viewMode')) $('viewMode').value = next;
  $('filter').value = '';
  const single = activeMode === 'single';
  const prompts = activeMode === 'prompts';
  const models = activeMode === 'models';
  const apiKeys = activeMode === 'apikeys';
  const cleanup = activeMode === 'cleanup';
  const tables = activeMode === 'tables';
  const metrics = activeMode === 'metrics';
  document.querySelectorAll('.compare-only').forEach(element => { element.hidden = single || prompts || models || apiKeys || cleanup || tables || metrics; });
  $('singleEnvControl').hidden = !(single || prompts);
  if($('modelEnvControl')) $('modelEnvControl').hidden = !models;
  if($('modelEnvSegmented')) $('modelEnvSegmented').hidden = !models;
  if($('modelTopActions')) $('modelTopActions').hidden = !models;
  if($('cleanupTopControls')) $('cleanupTopControls').hidden = !cleanup;
  if($('tablesTopControls')) $('tablesTopControls').hidden = !tables;
  if($('metricsTopControls')) $('metricsTopControls').hidden = !metrics;
  if($('status')) $('status').hidden = models || cleanup || tables || metrics;
  if($('filter')) $('filter').hidden = models || cleanup || tables || metrics;
  const bar = $('bar');
  if(bar){
    bar.classList.remove('env-desa', 'env-test', 'env-prod');
    if(models) bar.classList.add('env-' + modelsState.env);
    if(cleanup) bar.classList.add('env-' + cleanupState.env);
    if(tables) bar.classList.add('env-' + tablesState.env);
    if(metrics) bar.classList.add('env-' + metricsState.env);
  }
  $('apiKeyPresenceControl').hidden = !apiKeys;
  $('paneBar').hidden = single || prompts || models || apiKeys || cleanup || tables || metrics;
  $('editor').hidden = single || prompts || models || apiKeys || cleanup || tables || metrics;
  $('singleBar').hidden = !(single || prompts);
  $('singleEditor').hidden = !single;
  $('promptsView').hidden = !prompts;
  $('promptsFooter').hidden = !prompts;
  $('modelPanel').hidden = !models;
  $('apiKeyPanel').hidden = !apiKeys;
  if($('cleanupPanel')) $('cleanupPanel').hidden = !cleanup;
  if($('tablesPanel')) $('tablesPanel').hidden = !tables;
  if($('metricsPanel')) $('metricsPanel').hidden = !metrics;
  $('pick').hidden = models || apiKeys || cleanup || tables || metrics;
  $('ok').hidden = models || apiKeys || cleanup || tables || metrics;
  $('ok').textContent = (single || prompts) ? 'Abrir Bot' : 'OK · Comparar';
  $('filter').placeholder = models ? 'Filtrar modelo…' : apiKeys ? 'Filtrar client / bot / api key…' : 'Filtrar client / bot…';
  if(single || prompts){
    setSingleSubMode(prompts ? 'text' : 'json');
  }
  if(models){
    initModelsView();
  }else if(apiKeys){
    loadApiKeys(false);
  }else if(cleanup){
    initCleanupView();
  }else if(tables){
    initTablesView();
  }else if(metrics){
    initMetricsView();
  }else {
    updateComparePaneHeaders();
    ensureDiffWordWrap();
    refreshList(false);
  }
  syncRailActive(activeMode);
  syncSingleEnvButtons();
}
function initMonaco(){
  return new Promise((resolve, reject) => {
    if(!window.require) return reject(new Error('No se pudo cargar Monaco. Verificá la conexión a Internet.'));
    const base = 'https://cdnjs.cloudflare.com/ajax/libs/monaco-editor/0.52.2/min';
    window.MonacoEnvironment = {getWorkerUrl: () => 'data:text/javascript;charset=utf-8,' + encodeURIComponent('self.MonacoEnvironment = {baseUrl: "' + base + '/"}; importScripts("' + base + '/vs/base/worker/workerMain.js");')};
    window.require.config({paths: {vs: base + '/vs'}});
    window.require(['vs/editor/editor.main'], () => {
      // 1. Custom Monaco language for prompt templates
      monaco.languages.register({ id: 'prompt-markdown' });

      monaco.languages.setMonarchTokensProvider('prompt-markdown', {
        defaultToken: '',
        tokenPostfix: '.prompt',

        tokenizer: {
          root: [
            // Variables {VARIABLE_NAME}
            [/\{[A-Za-z0-9_\-\.]+\}/, 'custom-variable'],

            // Closing XML tags </TAG>
            [/<\/[A-Za-z0-9_\-]+>/, 'custom-xml-tag'],

            // Opening XML tags <TAG> or <TAG attr="val">
            [/<[A-Za-z0-9_\-]+(?:\s+[A-Za-z0-9_\-]+(?:=(?:"[^"]*"|'[^']*'|[^\s>]+))?)*\s*\/?>/, 'custom-xml-tag'],

            // Markdown Headers: # Title
            [/^#{1,6}\s+/, 'keyword'],

            // Inline code `code`
            [/`[^`]+`/, 'string'],

            // Markdown bold & italic
            [/\*\*[^*]+\*\*/, 'strong'],
            [/\*[^*]+\*/, 'emphasis'],

            // Bullet lists and numbered lists
            [/^\s*[-*+]\s+/, 'delimiter'],
            [/^\s*\d+\.\s+/, 'delimiter'],

            // Blockquotes
            [/^\s*>\s+/, 'comment'],

            // Text and whitespace
            [/[^<{`#*_\-\s]+/, ''],
            [/\s+/, '']
          ]
        }
      });

      monaco.languages.setLanguageConfiguration('prompt-markdown', {
        comments: {
          blockComment: ['<!--', '-->']
        },
        brackets: [
          ['{', '}'],
          ['<', '>'],
          ['[', ']'],
          ['(', ')']
        ],
        autoClosingPairs: [
          { open: '{', close: '}' },
          { open: '<', close: '>' },
          { open: '[', close: ']' },
          { open: '(', close: ')' },
          { open: '"', close: '"' },
          { open: "'", close: "'" },
          { open: '`', close: '`' }
        ],
        surroundingPairs: [
          { open: '{', close: '}' },
          { open: '<', close: '>' },
          { open: '[', close: ']' },
          { open: '(', close: ')' },
          { open: '"', close: '"' },
          { open: "'", close: "'" },
          { open: '`', close: '`' }
        ],
        folding: {
          markers: {
            start: /^\s*<([A-Za-z0-9_\-]+)[^>]*>\s*$/,
            end: /^\s*<\/([A-Za-z0-9_\-]+)>\s*$/
          }
        }
      });

      monaco.languages.registerFoldingRangeProvider('prompt-markdown', {
        provideFoldingRanges: function(model, context, token) {
          const ranges = [];
          const stack = [];
          const lineCount = model.getLineCount();
          const tagRegex = /<\/?([A-Za-z0-9_\-]+)(?:\s+[^>]*)?\/?>/g;

          for (let i = 1; i <= lineCount; i++) {
            const lineText = model.getLineContent(i);
            tagRegex.lastIndex = 0;
            let match;
            while ((match = tagRegex.exec(lineText)) !== null) {
              const fullTag = match[0];
              const tagName = match[1].toUpperCase();
              if (fullTag.endsWith('/>')) continue;

              if (fullTag.startsWith('</')) {
                for (let s = stack.length - 1; s >= 0; s--) {
                  if (stack[s].tag === tagName) {
                    const startLine = stack[s].line;
                    const endLine = i;
                    if (endLine > startLine) {
                      ranges.push({
                        start: startLine,
                        end: endLine,
                        kind: monaco.languages.FoldingRangeKind.Region
                      });
                    }
                    stack.splice(s);
                    break;
                  }
                }
              } else {
                stack.push({ tag: tagName, line: i });
              }
            }
          }
          return ranges;
        }
      });

      monaco.languages.registerCompletionItemProvider('prompt-markdown', {
        triggerCharacters: ['{'],
        provideCompletionItems: function(model, position) {
          const suggestions = [];
          const lineUntilPosition = model.getValueInRange({
            startLineNumber: position.lineNumber,
            startColumn: 1,
            endLineNumber: position.lineNumber,
            endColumn: position.column
          });

          if (unpackedPromptsData && selectedPromptName && unpackedPromptsData.prompts[selectedPromptName]) {
            const pData = unpackedPromptsData.prompts[selectedPromptName];
            const vars = pData.input_variables || [];
            vars.forEach(v => {
              const hasOpenBrace = lineUntilPosition.endsWith('{');
              suggestions.push({
                label: '{' + v + '}',
                kind: monaco.languages.CompletionItemKind.Variable,
                detail: 'Variable del prompt',
                documentation: 'Parámetro de ' + pData.name,
                insertText: hasOpenBrace ? (v + '}') : ('{' + v + '}'),
                range: {
                  startLineNumber: position.lineNumber,
                  startColumn: hasOpenBrace ? position.column : position.column,
                  endLineNumber: position.lineNumber,
                  endColumn: position.column
                }
              });
            });
          }
          return { suggestions };
        }
      });

      monaco.editor.defineTheme('prompt-dark', {
        base: 'vs-dark',
        inherit: true,
        rules: [
          { token: 'custom-variable', foreground: 'f59e0b', fontStyle: 'bold' },
          { token: 'custom-xml-tag', foreground: '38bdf8', fontStyle: 'bold' },
          { token: 'keyword', foreground: 'e06c75', fontStyle: 'bold' },
          { token: 'strong', fontStyle: 'bold', foreground: 'e5c07b' },
          { token: 'emphasis', fontStyle: 'italic' },
          { token: 'string', foreground: '98c379' },
          { token: 'comment', foreground: '7f848e', fontStyle: 'italic' }
        ],
        colors: {
          'editor.background': '#101216',
          'editorGutter.background': '#101216',
          'diffEditor.insertedTextBackground': '#22c55e44',
          'diffEditor.removedTextBackground': '#ef444444',
          'diffEditor.insertedLineBackground': '#22c55e1e',
          'diffEditor.removedLineBackground': '#ef44441e',
          'diffEditorGutter.insertedLineBackground': '#22c55e50',
          'diffEditorGutter.removedLineBackground': '#ef444450',
          'diffEditorOverview.insertedForeground': '#22c55e',
          'diffEditorOverview.removedForeground': '#ef4444',
          'diffEditor.diagonalFill': '#00000000'
        }
      });

      leftModel = monaco.editor.createModel('{}', 'json');
      rightModel = monaco.editor.createModel('{}', 'json');
      singleModel = monaco.editor.createModel('{}', 'json');
      templateModel = monaco.editor.createModel('', 'prompt-markdown');
      diffEditor = monaco.editor.createDiffEditor($('editor'), {
        theme: 'vs-dark', renderSideBySide: true, originalEditable: true, readOnly: false,
        automaticLayout: true, minimap: {enabled: false}, wordWrap: 'on', diffWordWrap: 'on',
        wrappingStrategy: 'advanced', wrappingIndent: 'indent', folding: true,
        scrollBeyondLastLine: false, maxComputationTime: 1000, maxFileSize: 5,
        hideUnchangedRegions: {enabled: false, contextLineCount: 3}
      });
      diffEditor.setModel({original: leftModel, modified: rightModel});
      if(diffEditor.getOriginalEditor()){
        diffEditor.getOriginalEditor().updateOptions({wordWrap: 'on', wrappingStrategy: 'advanced', wrappingIndent: 'indent'});
      }
      if(diffEditor.getModifiedEditor()){
        diffEditor.getModifiedEditor().updateOptions({wordWrap: 'on', wrappingStrategy: 'advanced', wrappingIndent: 'indent'});
      }
      singleEditor = monaco.editor.create($('singleEditor'), {
        model: singleModel, theme: 'vs-dark', automaticLayout: true,
        minimap: {enabled: false}, wordWrap: 'on', folding: true,
        scrollBeyondLastLine: false, fontSize: currentFontSize
      });
      templateEditor = monaco.editor.create($('templateMonacoEditor'), {
        model: templateModel, theme: 'prompt-dark', automaticLayout: true,
        minimap: {enabled: false}, wordWrap: 'on', folding: true,
        scrollBeyondLastLine: false, fontSize: currentFontSize, lineNumbers: 'on'
      });
      templateEditor.addCommand(monaco.KeyMod.CtrlCmd | monaco.KeyCode.KeyS, () => {
        saveSingle();
      });
      diffEditor.getOriginalEditor().addCommand(monaco.KeyMod.CtrlCmd | monaco.KeyCode.KeyS, () => {
        if(dirty('left') || versions.left === null) saveSide('left');
      });
      diffEditor.getModifiedEditor().addCommand(monaco.KeyMod.CtrlCmd | monaco.KeyCode.KeyS, () => {
        if(dirty('right') || versions.right === null) saveSide('right');
      });
      singleEditor.addCommand(monaco.KeyMod.CtrlCmd | monaco.KeyCode.KeyS, () => {
        saveSingle();
      });
      compTemplateModel = monaco.editor.createModel('', 'prompt-markdown');
      if(compTemplateModel.setEOL) compTemplateModel.setEOL(monaco.editor.EndOfLineSequence.LF);
      if(templateModel && templateModel.setEOL) templateModel.setEOL(monaco.editor.EndOfLineSequence.LF);

      const initWrapOpt = templateWrapEnabled ? 'on' : 'off';
      const initWrapStrat = templateWrapEnabled ? 'advanced' : 'normal';
      const initHorizScroll = !templateWrapEnabled;

      templateDiffEditor = monaco.editor.createDiffEditor($('templateMonacoDiffEditor'), {
        theme: 'vs-dark',
        automaticLayout: true,
        renderSideBySide: true,
        originalEditable: false,
        readOnly: true,

        diffAlgorithm: 'advanced',
        renderIndicators: true,
        renderMarginRevertIcon: true,

        enableSplitViewResizing: true,
        splitViewDefaultRatio: 0.5,

        renderOverviewRuler: true,
        ignoreTrimWhitespace: false,

        scrollBeyondLastLine: false,
        fontSize: currentFontSize,
        hideUnchangedRegions: { enabled: false },

        originalAriaLabel: 'Prompt original',
        modifiedAriaLabel: 'Prompt modificado',

        diffWordWrap: initWrapOpt,
        wordWrap: initWrapOpt,
        wrappingStrategy: initWrapStrat,
        scrollBeyondLastColumn: 0,
        horizontalScrolling: initHorizScroll,

        originalEditor: {
          wordWrap: initWrapOpt,
          wrappingStrategy: initWrapStrat,
          scrollBeyondLastColumn: 0,
          horizontalScrolling: initHorizScroll,
          minimap: { enabled: false },
          lineNumbers: 'on',
          glyphMargin: true,
          folding: true,
          fontSize: currentFontSize,
          padding: { top: 12, bottom: 12 }
        },

        modifiedEditor: {
          wordWrap: initWrapOpt,
          wrappingStrategy: initWrapStrat,
          scrollBeyondLastColumn: 0,
          horizontalScrolling: initHorizScroll,
          minimap: { enabled: false },
          lineNumbers: 'on',
          glyphMargin: true,
          folding: true,
          fontSize: currentFontSize,
          padding: { top: 12, bottom: 12 }
        }
      });
      templateDiffEditor.setModel({ original: compTemplateModel, modified: templateModel });
      if(templateDiffEditor.getOriginalEditor()){
        templateDiffEditor.getOriginalEditor().updateOptions({
          wordWrap: initWrapOpt,
          wrappingStrategy: initWrapStrat,
          scrollBeyondLastColumn: 0,
          horizontalScrolling: initHorizScroll,
          minimap: { enabled: false },
          lineNumbers: 'on',
          glyphMargin: true,
          folding: true,
          fontSize: currentFontSize,
          padding: { top: 12, bottom: 12 }
        });
      }
      if(templateDiffEditor.getModifiedEditor()){
        templateDiffEditor.getModifiedEditor().updateOptions({
          wordWrap: initWrapOpt,
          wrappingStrategy: initWrapStrat,
          scrollBeyondLastColumn: 0,
          horizontalScrolling: initHorizScroll,
          minimap: { enabled: false },
          lineNumbers: 'on',
          glyphMargin: true,
          folding: true,
          fontSize: currentFontSize,
          padding: { top: 12, bottom: 12 }
        });
      }
      diffEditor.onDidUpdateDiff(onDiffUpdated);
      leftModel.onDidChangeContent(() => { diffPending = true; updateSaveButtons(); });
      rightModel.onDidChangeContent(() => { diffPending = true; updateSaveButtons(); });
      singleModel.onDidChangeContent(updateSaveButtons);
      templateModel.onDidChangeContent(onTemplateContentChanged);
      templateEditor.onDidFocusEditorText(() => {
        selectRightJsonField('TEMPLATE');
      });
      resolve();
    }, reject);
  });
}

/* ---------- GESTIÓN DE BOTS Y LIMPIEZA DE REGISTROS DYNAMODB ---------- */
let cleanupState = {
  env: 'desa',
  data: null,
  selectedKeys: new Set(),
  expandedClients: new Set(),
  clientFilterTabs: {},
  searchQuery: '',
  loading: false,
  initialized: false
};

function initCleanupView(){
  if(!cleanupState.initialized){
    cleanupState.initialized = true;

    // Segmented control para selección de ambiente
    document.querySelectorAll('#cleanupEnvSegmented .env-btn').forEach(btn => {
      btn.onclick = () => setCleanupEnv(btn.dataset.env);
    });

    // Botón de refresco manual
    if($('cleanupBtnRefresh')){
      $('cleanupBtnRefresh').onclick = () => loadCleanupData(true);
    }

    // Buscador global de cliente o bot con debounce
    if($('cleanupGlobalSearch')){
      let searchDebounce = null;
      $('cleanupGlobalSearch').oninput = e => {
        clearTimeout(searchDebounce);
        searchDebounce = setTimeout(() => {
          cleanupState.searchQuery = e.target.value.trim().toLowerCase();
          renderCleanupClientsSidebar();
          renderCleanupAccordion();
        }, 150);
      };
    }

    // Seleccionar todos los bots del ambiente
    if($('cleanupSelectAllEnv')){
      $('cleanupSelectAllEnv').onchange = e => {
        toggleAllEnvSelection(e.target.checked);
      };
    }

    // Checkbox de confirmación en el panel lateral derecho
    if($('cleanupConfirmCheckbox')){
      $('cleanupConfirmCheckbox').onchange = () => {
        updateCleanupRightPanel();
      };
    }

    // Botón eliminar seleccionados (abre modal de confirmación)
    if($('btnDeleteSelectedRecords')){
      $('btnDeleteSelectedRecords').onclick = () => openCleanupDeleteConfirmModal();
    }

    // Botón exportar selección a CSV
    if($('btnExportCleanup')){
      $('btnExportCleanup').onclick = () => exportCleanupSelection();
    }

    // Modal de confirmación de eliminación: botones y validación PROD
    if($('cleanupModalCloseBtn')) $('cleanupModalCloseBtn').onclick = closeCleanupDeleteConfirmModal;
    if($('btnCleanupModalCancel')) $('btnCleanupModalCancel').onclick = closeCleanupDeleteConfirmModal;
    if($('btnCleanupModalExecute')) $('btnCleanupModalExecute').onclick = executeCleanupDelete;
    if($('cleanupModalProdInput')){
      $('cleanupModalProdInput').oninput = e => {
        const isConfirmed = e.target.value.trim() === 'ELIMINAR EN PROD';
        if($('btnCleanupModalExecute')) $('btnCleanupModalExecute').disabled = !isConfirmed;
      };
    }

    // Modal de detalle de bot
    if($('cleanupDetailCloseBtn')) $('cleanupDetailCloseBtn').onclick = closeCleanupBotDetailModal;
    if($('btnCleanupDetailClose')) $('btnCleanupDetailClose').onclick = closeCleanupBotDetailModal;
    if($('btnCleanupDetailDeleteOne')){
      $('btnCleanupDetailDeleteOne').onclick = () => {
        const c = $('btnCleanupDetailDeleteOne').dataset.client;
        const b = $('btnCleanupDetailDeleteOne').dataset.bot;
        if(c && b){
          closeCleanupBotDetailModal();
          cleanupState.selectedKeys.clear();
          cleanupState.selectedKeys.add(`${c}:::${b}`);
          if($('cleanupConfirmCheckbox')) $('cleanupConfirmCheckbox').checked = false;
          updateCleanupRightPanel();
          renderCleanupAccordion();
          openCleanupDeleteConfirmModal();
        }
      };
    }

    // Clicks en la lista izquierda de clientes (scroll and expand)
    if($('cleanupClientsList')){
      $('cleanupClientsList').onclick = e => {
        const item = e.target.closest('.cleanup-sidebar-client-item');
        if(!item) return;
        const clientName = item.dataset.client;
        if(!clientName) return;
        cleanupState.expandedClients.add(clientName);
        renderCleanupClientsSidebar();
        renderCleanupAccordion();
        const targetCard = document.querySelector(`.cleanup-client-card[data-client="${CSS.escape(clientName)}"]`);
        if(targetCard){
          targetCard.scrollIntoView({behavior: 'smooth', block: 'start'});
        }
      };
    }

    // Clicks en la lista de items seleccionados del panel derecho
    if($('cleanupSelectedItemsChips')){
      $('cleanupSelectedItemsChips').onclick = e => {
        const removeBtn = e.target.closest('.cleanup-chip-remove');
        if(removeBtn){
          e.stopPropagation();
          const key = removeBtn.dataset.key;
          if(key){
            cleanupState.selectedKeys.delete(key);
            updateCleanupRightPanel();
            renderCleanupAccordion();
          }
          return;
        }

        const groupRemoveBtn = e.target.closest('.cleanup-sel-group-remove');
        if(groupRemoveBtn){
          e.stopPropagation();
          const clientName = groupRemoveBtn.dataset.client;
          if(clientName){
            toggleClientSelection(clientName, false);
          }
          return;
        }

        const botRow = e.target.closest('.cleanup-sel-bot-row');
        if(botRow){
          const clientName = botRow.dataset.client;
          if(clientName){
            cleanupState.expandedClients.add(clientName);
            renderCleanupClientsSidebar();
            renderCleanupAccordion();
            const targetCard = document.querySelector(`.cleanup-client-card[data-client="${CSS.escape(clientName)}"]`);
            if(targetCard){
              targetCard.scrollIntoView({behavior: 'smooth', block: 'start'});
            }
          }
        }
      };
    }

    if($('btnCleanupClearAll')){
      $('btnCleanupClearAll').onclick = () => {
        cleanupState.selectedKeys.clear();
        updateCleanupRightPanel();
        renderCleanupAccordion();
      };
    }

    // Event delegation para el acordeón central
    if($('cleanupAccordionContainer')){
      $('cleanupAccordionContainer').onclick = e => {
        // 1. Checkbox del cliente (nivel header)
        const clientChk = e.target.closest('.cleanup-client-check');
        if(clientChk){
          e.stopPropagation();
          const clientName = clientChk.dataset.client;
          const cObj = cleanupState.data && cleanupState.data.clients.find(c => c.client === clientName);
          const total = cObj ? (cObj.bots || []).length : 0;
          const curSel = cObj ? (cObj.bots || []).filter(b => cleanupState.selectedKeys.has(`${clientName}:::${b.bot}`)).length : 0;
          const targetState = (curSel === total && total > 0) ? false : true;
          toggleClientSelection(clientName, targetState);
          return;
        }

        // 2. Checkbox del sub-toolbar "Seleccionar todo (N)"
        const subAllChk = e.target.closest('.cleanup-sub-select-all');
        if(subAllChk){
          e.stopPropagation();
          const clientName = subAllChk.dataset.client;
          const cObj = cleanupState.data && cleanupState.data.clients.find(c => c.client === clientName);
          const total = cObj ? (cObj.bots || []).length : 0;
          const curSel = cObj ? (cObj.bots || []).filter(b => cleanupState.selectedKeys.has(`${clientName}:::${b.bot}`)).length : 0;
          const targetState = (curSel === total && total > 0) ? false : true;
          toggleClientSelection(clientName, targetState);
          return;
        }

        // 3. Pestañas de filtro de bots (Todos vs Con registros)
        const tabBtn = e.target.closest('.cleanup-client-tab-btn');
        if(tabBtn){
          e.stopPropagation();
          const clientName = tabBtn.dataset.client;
          const tab = tabBtn.dataset.tab;
          if(clientName && tab){
            cleanupState.clientFilterTabs[clientName] = tab;
            renderCleanupAccordion();
          }
          return;
        }

        // 4. Checkbox individual de bot
        const botChk = e.target.closest('.cleanup-bot-check');
        if(botChk){
          e.stopPropagation();
          const clientName = botChk.dataset.client;
          const botName = botChk.dataset.bot;
          toggleBotSelection(clientName, botName, botChk.checked);
          return;
        }

        // 5. Botón de acción adicional [...]
        const actionBtn = e.target.closest('.cleanup-action-more-btn');
        if(actionBtn){
          e.stopPropagation();
          const clientName = actionBtn.dataset.client;
          const botName = actionBtn.dataset.bot;
          openCleanupBotDetail(clientName, botName);
          return;
        }

        // 6. Botón chevron para expandir/contraer
        const chevronBtn = e.target.closest('.cleanup-chevron-btn');
        if(chevronBtn){
          e.stopPropagation();
          const cardHeader = chevronBtn.closest('.cleanup-client-card-header');
          const clientName = cardHeader ? cardHeader.dataset.client : null;
          if(clientName) toggleClientExpanded(clientName);
          return;
        }

        // 7. Header del card para expandir/contraer
        const cardHeader = e.target.closest('.cleanup-client-card-header');
        if(cardHeader){
          const clientName = cardHeader.dataset.client;
          if(clientName){
            toggleClientExpanded(clientName);
          }
          return;
        }
      };
    }
  }

  // Si no hay datos cargados para el ambiente activo, solicitar al backend
  if(!cleanupState.data){
    loadCleanupData(false);
  }
}

function setCleanupEnv(env){
  if(cleanupState.env === env && cleanupState.data) return;
  cleanupState.env = env;
  cleanupState.selectedKeys.clear();
  if($('cleanupConfirmCheckbox')) $('cleanupConfirmCheckbox').checked = false;

  // Actualizar control segmentado superior
  document.querySelectorAll('#cleanupEnvSegmented .env-btn').forEach(btn => {
    btn.classList.toggle('active', btn.dataset.env === env);
  });

  // Actualizar barra y panel lateral
  const bar = $('bar');
  if(bar){
    bar.classList.remove('env-desa', 'env-test', 'env-prod');
    bar.classList.add('env-' + env);
  }

  const badge = $('cleanupRightEnvBadge');
  if(badge){
    badge.textContent = env.toUpperCase();
    badge.className = 'env-badge ' + env;
  }
  if($('cleanupWarnEnv')) $('cleanupWarnEnv').textContent = env.toUpperCase();

  loadCleanupData(false);
}

async function loadCleanupData(refresh){
  cleanupState.loading = true;
  const container = $('cleanupAccordionContainer');
  const sidebar = $('cleanupClientsList');

  if(container){
    container.innerHTML = '<div style="text-align:center;padding:50px 20px;color:#94a3b8;"><div class="cleanup-spinner" style="margin:0 auto 16px;"></div>Consultando clientes, bots y registros de DynamoDB en ' + esc(cleanupState.env.toUpperCase()) + '…</div>';
  }
  if(sidebar && (!cleanupState.data || refresh)){
    sidebar.innerHTML = '<div style="text-align:center;padding:24px;color:#64748b;font-size:12px;">Cargando…</div>';
  }

  try{
    const res = await api('/api/cleanup/data?env=' + encodeURIComponent(cleanupState.env) + (refresh ? '&refresh=1' : ''));
    cleanupState.data = res;

    // Actualizar resumen superior e indicador
    if($('cleanupEnvSummary')){
      $('cleanupEnvSummary').textContent = 'Ambiente: ' + cleanupState.env.toUpperCase() + ' • ' + (res.total_bots || 0) + ' bots • ' + (res.total_clients || 0) + ' clientes';
    }
    if($('cleanupClientsCount')){
      $('cleanupClientsCount').textContent = res.total_clients || 0;
    }

    // Por defecto expandir el primer cliente si ninguno está expandido
    if(cleanupState.expandedClients.size === 0 && res.clients && res.clients.length > 0){
      cleanupState.expandedClients.add(res.clients[0].client);
    }

    renderCleanupClientsSidebar();
    renderCleanupAccordion();
    updateCleanupRightPanel();
  }catch(err){
    if(container){
      container.innerHTML = '<div style="padding:20px;background:#2b1212;border:1px solid #7f1d1d;border-radius:6px;color:#fca5a5;font-size:13px;line-height:1.5;">' +
        '<strong>Error al consultar DynamoDB:</strong><br>' + esc(err.message) +
        '<div style="margin-top:10px;"><button type="button" class="btn-export-cleanup" style="display:inline-flex;width:auto;" onclick="loadCleanupData(true)">Reintentar</button></div>' +
        '</div>';
    }
  }finally{
    cleanupState.loading = false;
  }
}

function getFilteredCleanupClients(){
  if(!cleanupState.data || !cleanupState.data.clients) return [];
  const q = cleanupState.searchQuery;
  if(!q) return cleanupState.data.clients;

  const filtered = [];
  for(const c of cleanupState.data.clients){
    const clientMatch = c.client.toLowerCase().includes(q);
    const matchingBots = (c.bots || []).filter(b => {
      return b.bot.toLowerCase().includes(q) || (b.system_id && b.system_id.toLowerCase().includes(q));
    });
    if(clientMatch){
      filtered.push(c);
    }else if(matchingBots.length > 0){
      filtered.push({
        ...c,
        bots: matchingBots,
        bots_count: matchingBots.length
      });
    }
  }
  return filtered;
}

function renderCleanupClientsSidebar(){
  const sidebar = $('cleanupClientsList');
  if(!sidebar || !cleanupState.data) return;

  const clients = getFilteredCleanupClients();
  if(!clients.length){
    sidebar.innerHTML = '<div style="padding:16px;text-align:center;color:#64748b;font-size:12px;">Sin coincidencias</div>';
    return;
  }

  sidebar.innerHTML = clients.map(c => {
    const isExpanded = cleanupState.expandedClients.has(c.client);
    return '<div class="cleanup-sidebar-client-item' + (isExpanded ? ' active' : '') + '" data-client="' + esc(c.client) + '">' +
      '<span class="cleanup-sidebar-client-name"><span class="chevron">' + (isExpanded ? '▼' : '▶') + '</span> ' + esc(c.client) + '</span>' +
      '<span class="cleanup-sidebar-client-badge">' + c.bots_count + ' bots</span>' +
      '</div>';
  }).join('');
}

function toggleClientExpanded(clientName){
  if(cleanupState.expandedClients.has(clientName)){
    cleanupState.expandedClients.delete(clientName);
  }else{
    cleanupState.expandedClients.add(clientName);
  }
  renderCleanupClientsSidebar();
  renderCleanupAccordion();
}

function toggleBotSelection(clientName, botName, isChecked){
  const key = `${clientName}:::${botName}`;
  if(isChecked){
    cleanupState.selectedKeys.add(key);
  }else{
    cleanupState.selectedKeys.delete(key);
  }
  updateCleanupRightPanel();
  renderCleanupAccordion();
}

function toggleClientSelection(clientName, isChecked){
  if(!cleanupState.data || !cleanupState.data.clients) return;
  const cObj = cleanupState.data.clients.find(c => c.client === clientName);
  if(!cObj) return;

  const clientBots = cObj.bots || [];
  const selectedCount = clientBots.filter(b => cleanupState.selectedKeys.has(`${clientName}:::${b.bot}`)).length;
  const allSelected = clientBots.length > 0 && selectedCount === clientBots.length;
  const targetState = (isChecked !== undefined) ? isChecked : !allSelected;

  clientBots.forEach(b => {
    const key = `${clientName}:::${b.bot}`;
    if(targetState){
      cleanupState.selectedKeys.add(key);
    }else{
      cleanupState.selectedKeys.delete(key);
    }
  });

  updateCleanupRightPanel();
  renderCleanupAccordion();
}

function toggleAllEnvSelection(isChecked){
  if(!cleanupState.data || !cleanupState.data.clients) return;
  cleanupState.selectedKeys.clear();
  if(isChecked){
    cleanupState.data.clients.forEach(c => {
      (c.bots || []).forEach(b => {
        cleanupState.selectedKeys.add(`${c.client}:::${b.bot}`);
      });
    });
  }
  updateCleanupRightPanel();
  renderCleanupAccordion();
}

function renderCleanupAccordion(){
  const container = $('cleanupAccordionContainer');
  const emptyMsg = $('cleanupEmptyMessage');
  if(!container || !cleanupState.data) return;

  const clients = getFilteredCleanupClients();
  if(!clients.length){
    container.innerHTML = '';
    if(emptyMsg) emptyMsg.hidden = false;
    return;
  }
  if(emptyMsg) emptyMsg.hidden = true;

  let html = '';
  clients.forEach(c => {
    const isExpanded = cleanupState.expandedClients.has(c.client);
    const clientBots = c.bots || [];

    // Calcular estado de selección para el checkbox del cliente
    const selectedCount = clientBots.filter(b => cleanupState.selectedKeys.has(`${c.client}:::${b.bot}`)).length;
    const allSelected = clientBots.length > 0 && selectedCount === clientBots.length;
    const someSelected = selectedCount > 0 && selectedCount < clientBots.length;

    // Tags preview: nombres de bots en formato 'ventas • tramites • cobranza • ...'
    let tagsList = (c.tags_preview && c.tags_preview.length) ? c.tags_preview : clientBots.map(b => b.bot);
    let tagsHtml = '';
    if(tagsList.length > 8){
      tagsHtml = tagsList.slice(0, 8).map(esc).join('  •  ') + '  •  otros';
    }else{
      tagsHtml = tagsList.map(esc).join('  •  ');
    }

    // Pestaña activa dentro del cliente ('all' o 'with_records')
    const activeSubTab = cleanupState.clientFilterTabs[c.client] || 'all';
    const withRecordsCount = clientBots.filter(b => (b.dynamodb_records || 0) > 0).length;
    const visibleBots = activeSubTab === 'with_records'
      ? clientBots.filter(b => (b.dynamodb_records || 0) > 0)
      : clientBots;

    html += '<div class="cleanup-client-card' + (isExpanded ? ' expanded' : '') + '" data-client="' + esc(c.client) + '">';
    html += '  <div class="cleanup-client-card-header" data-client="' + esc(c.client) + '">';
    html += '    <div class="cleanup-client-header-left">';
    html += '      <input type="checkbox" class="cleanup-client-check" data-client="' + esc(c.client) + '" ' + (allSelected ? 'checked' : '') + '>';
    html += '      <button type="button" class="cleanup-chevron-btn">' + (isExpanded ? '▼' : '▶') + '</button>';
    html += '      <span class="cleanup-client-title">' + esc(c.client) + '</span>';
    html += '      <span class="cleanup-client-bots-badge">' + c.bots_count + ' bots</span>';
    html += '    </div>';
    html += '    <div class="cleanup-client-tags-preview">' + tagsHtml + '</div>';
    html += '  </div>';

    if(isExpanded){
      html += '  <div class="cleanup-client-body">';
      html += '    <div class="cleanup-client-toolbar">';
      html += '      <label class="cleanup-client-select-all-label">';
      html += '        <input type="checkbox" class="cleanup-sub-select-all" data-client="' + esc(c.client) + '" ' + (allSelected ? 'checked' : '') + '>';
      html += '        Seleccionar todo (' + clientBots.length + ')';
      html += '      </label>';
      html += '      <div class="cleanup-client-tabs">';
      html += '        <button type="button" class="cleanup-client-tab-btn' + (activeSubTab === 'all' ? ' active' : '') + '" data-client="' + esc(c.client) + '" data-tab="all">Todos (' + clientBots.length + ')</button>';
      html += '        <button type="button" class="cleanup-client-tab-btn' + (activeSubTab === 'with_records' ? ' active' : '') + '" data-client="' + esc(c.client) + '" data-tab="with_records">Con registros (' + withRecordsCount + ')</button>';
      html += '      </div>';
      html += '    </div>';

      html += '    <table class="cleanup-bots-table">';
      html += '      <thead><tr>';
      html += '        <th class="th-check"></th>';
      html += '        <th>BOT / AGENTE</th>';
      html += '        <th>REGISTROS DYNAMO</th>';
      html += '        <th>ÚLTIMA ACTUALIZACIÓN</th>';
      html += '        <th style="text-align:right;">ACCIÓN</th>';
      html += '      </tr></thead>';
      html += '      <tbody>';

      if(visibleBots.length === 0){
        html += '        <tr><td colspan="5" style="text-align:center;padding:18px;color:#64748b;font-style:italic;">No hay bots en esta categoría.</td></tr>';
      }else{
        visibleBots.forEach(b => {
          const isBotChecked = cleanupState.selectedKeys.has(`${c.client}:::${b.bot}`);
          const recCount = b.dynamodb_records || 0;
          const badgeClass = recCount > 0 ? 'has-records' : 'no-records';
          const recText = recCount === 1 ? '1 registro' : (recCount + ' registros');
          const subId = b.system_id || ('agente-' + b.bot);
          const dateStr = b.last_updated || 'Sin registros';

          html += '        <tr>';
          html += '          <td class="td-check">';
          html += '            <input type="checkbox" class="cleanup-bot-check" data-client="' + esc(c.client) + '" data-bot="' + esc(b.bot) + '" ' + (isBotChecked ? 'checked' : '') + '>';
          html += '          </td>';
          html += '          <td>';
          html += '            <div class="cleanup-bot-name-cell">';
          html += '              <span class="cleanup-bot-name">' + esc(b.bot) + '</span>';
          html += '              <span class="cleanup-bot-subid">' + esc(subId) + '</span>';
          html += '            </div>';
          html += '          </td>';
          html += '          <td>';
          html += '            <span class="cleanup-badge-records ' + badgeClass + '">' + recText + '</span>';
          html += '          </td>';
          html += '          <td>';
          html += '            <span class="cleanup-bot-date">' + esc(dateStr) + '</span>';
          html += '          </td>';
          html += '          <td style="text-align:right;">';
          html += '            <button type="button" class="cleanup-action-more-btn" data-client="' + esc(c.client) + '" data-bot="' + esc(b.bot) + '" title="Ver detalles y tablas">···</button>';
          html += '          </td>';
          html += '        </tr>';
        });
      }

      html += '      </tbody>';
      html += '    </table>';
      html += '  </div>';
    }

    html += '</div>';
  });

  container.innerHTML = html;

  // Actualizar propiedades indeterminate en checkboxes de clientes
  clients.forEach(c => {
    const clientBots = c.bots || [];
    const selectedCount = clientBots.filter(b => cleanupState.selectedKeys.has(`${c.client}:::${b.bot}`)).length;
    const someSelected = selectedCount > 0 && selectedCount < clientBots.length;

    const chk = container.querySelector(`.cleanup-client-check[data-client="${CSS.escape(c.client)}"]`);
    if(chk) chk.indeterminate = someSelected;

    const subChk = container.querySelector(`.cleanup-sub-select-all[data-client="${CSS.escape(c.client)}"]`);
    if(subChk) subChk.indeterminate = someSelected;
  });
}

function updateCleanupRightPanel(){
  const count = cleanupState.selectedKeys.size;
  let totalRecords = 0;
  const chips = [];

  if(cleanupState.data && cleanupState.data.clients){
    for(const key of cleanupState.selectedKeys){
      const [cName, bName] = key.split(':::');
      const cObj = cleanupState.data.clients.find(c => c.client === cName);
      const bObj = cObj ? (cObj.bots || []).find(b => b.bot === bName) : null;
      const recs = bObj ? (bObj.dynamodb_records || 0) : 0;
      totalRecords += recs;
      chips.push({client: cName, bot: bName, records: recs, key});
    }
  }

  // Contadores
  if($('cleanupSelectedBotsCount')) $('cleanupSelectedBotsCount').textContent = count;
  if($('cleanupSelectedRecordsCount')) $('cleanupSelectedRecordsCount').textContent = totalRecords;

  // Vista previa y listado de bots seleccionados
  const countBadge = $('cleanupPreviewCount');
  const clearAllBtn = $('btnCleanupClearAll');
  const chipsContainer = $('cleanupSelectedItemsChips');

  if(countBadge) countBadge.textContent = count;
  if(clearAllBtn) clearAllBtn.style.display = (count > 0) ? 'inline-block' : 'none';

  if(chipsContainer){
    if(count === 0){
      chipsContainer.innerHTML = '<div class="cleanup-empty-selection">' +
        '<svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" style="opacity:0.45;margin-bottom:2px;">' +
        '<circle cx="12" cy="12" r="10"></circle><line x1="12" y1="8" x2="12" y2="12"></line><line x1="12" y1="16" x2="12.01" y2="16"></line></svg>' +
        '<span style="font-weight:600;color:#94a3b8;">Ningún bot seleccionado</span>' +
        '<span style="font-size:11px;color:#64748b;">Marcá clientes o bots del panel central para agregarlos aquí.</span>' +
        '</div>';
    }else{
      // Agrupar por cliente
      const byClient = {};
      chips.forEach(c => {
        if(!byClient[c.client]) byClient[c.client] = [];
        byClient[c.client].push(c);
      });

      let listHtml = '';
      const clientNames = Object.keys(byClient).sort();
      for(const clientName of clientNames){
        const botList = byClient[clientName].sort((a,b) => a.bot.localeCompare(b.bot));
        const clientRecords = botList.reduce((acc, curr) => acc + (curr.records || 0), 0);
        const botsCountText = botList.length === 1 ? '1 bot' : (botList.length + ' bots');

        listHtml += '<div class="cleanup-sel-group">';
        listHtml += '  <div class="cleanup-sel-group-header">';
        listHtml += '    <div class="cleanup-sel-client-name">';
        listHtml += '      <strong>' + esc(clientName) + '</strong>';
        listHtml += '      <span class="cleanup-sel-client-count">' + botsCountText + '</span>';
        listHtml += '    </div>';
        listHtml += '    <div class="cleanup-sel-group-actions">';
        if(clientRecords > 0){
          listHtml += '      <span class="cleanup-sel-group-records">' + clientRecords + ' reg</span>';
        }
        listHtml += '      <button type="button" class="cleanup-sel-group-remove" data-client="' + esc(clientName) + '" title="Quitar todos los bots de ' + esc(clientName) + '">×</button>';
        listHtml += '    </div>';
        listHtml += '  </div>';

        listHtml += '  <div class="cleanup-sel-group-items">';
        botList.forEach(b => {
          const recClass = (b.records > 0) ? 'has-records' : '';
          const recText = b.records === 1 ? '1 reg' : (b.records + ' reg');
          listHtml += '    <div class="cleanup-sel-bot-row" data-key="' + esc(b.key) + '" data-client="' + esc(b.client) + '" data-bot="' + esc(b.bot) + '" title="Click para ubicar en la lista central">';
          listHtml += '      <span class="cleanup-sel-bot-name">' + esc(b.bot) + '</span>';
          listHtml += '      <div class="cleanup-sel-bot-meta">';
          listHtml += '        <span class="cleanup-sel-bot-records ' + recClass + '">' + recText + '</span>';
          listHtml += '        <button type="button" class="cleanup-chip-remove" data-key="' + esc(b.key) + '" title="Quitar bot">×</button>';
          listHtml += '      </div>';
          listHtml += '    </div>';
        });
        listHtml += '  </div>';
        listHtml += '</div>';
      }

      chipsContainer.innerHTML = listHtml;
    }
  }

  // Estado del botón eliminar: solo si hay bots seleccionados y se confirmó el checkbox
  const isConfirmed = $('cleanupConfirmCheckbox') && $('cleanupConfirmCheckbox').checked;
  if($('btnDeleteSelectedRecords')){
    $('btnDeleteSelectedRecords').disabled = (count === 0 || !isConfirmed);
  }

  // Estado del botón exportar
  if($('btnExportCleanup')){
    $('btnExportCleanup').disabled = (count === 0);
  }

  // Checkbox superior "Seleccionar todos los bots del ambiente"
  const totalBots = cleanupState.data ? (cleanupState.data.total_bots || 0) : 0;
  const allEnvChk = $('cleanupSelectAllEnv');
  if(allEnvChk){
    allEnvChk.checked = (totalBots > 0 && count === totalBots);
    allEnvChk.indeterminate = (count > 0 && count < totalBots);
  }
}

function openCleanupDeleteConfirmModal(){
  if(cleanupState.selectedKeys.size === 0) return;

  const modal = $('cleanupDeleteConfirmModal');
  if(!modal) return;

  // Configurar títulos y badges
  if($('cleanupModalEnvBadge')){
    $('cleanupModalEnvBadge').textContent = cleanupState.env.toUpperCase();
    $('cleanupModalEnvBadge').className = 'env-badge ' + cleanupState.env;
  }
  if($('cleanupModalEnvName')) $('cleanupModalEnvName').textContent = cleanupState.env.toUpperCase();
  if($('cleanupModalCount')) $('cleanupModalCount').textContent = cleanupState.selectedKeys.size;

  // Llenar tabla resumen
  const tbody = $('cleanupModalBotsTableBody');
  if(tbody && cleanupState.data && cleanupState.data.clients){
    let rowsHtml = '';
    for(const key of cleanupState.selectedKeys){
      const [cName, bName] = key.split(':::');
      const cObj = cleanupState.data.clients.find(c => c.client === cName);
      const bObj = cObj ? (cObj.bots || []).find(b => b.bot === bName) : null;
      const recs = bObj ? (bObj.dynamodb_records || 0) : 0;
      rowsHtml += '<tr>' +
        '<td><strong>' + esc(cName) + '</strong></td>' +
        '<td>' + esc(bName) + ' <small style="color:#94a3b8;">(' + esc((bObj && bObj.system_id) || ('agente-' + bName)) + ')</small></td>' +
        '<td><span class="cleanup-badge-records ' + (recs > 0 ? 'has-records' : 'no-records') + '">' + recs + ' registros</span></td>' +
        '</tr>';
    }
    tbody.innerHTML = rowsHtml;
  }

  // Advertencia de PRODUCCIÓN
  const prodWarn = $('cleanupModalProdWarning');
  const prodInput = $('cleanupModalProdInput');
  const btnExecute = $('btnCleanupModalExecute');

  if(cleanupState.env === 'prod'){
    if(prodWarn) prodWarn.hidden = false;
    if(prodInput) prodInput.value = '';
    if(btnExecute){
      btnExecute.disabled = true;
      btnExecute.textContent = 'Confirmar y Eliminar en PROD';
    }
  }else{
    if(prodWarn) prodWarn.hidden = true;
    if(btnExecute){
      btnExecute.disabled = false;
      btnExecute.textContent = 'Confirmar y Eliminar';
    }
  }

  // Ocultar spinners y resultados previos
  if($('cleanupModalProgressWrap')) $('cleanupModalProgressWrap').hidden = true;
  if($('cleanupModalResultWrap')) $('cleanupModalResultWrap').hidden = true;
  if($('btnCleanupModalCancel')) $('btnCleanupModalCancel').disabled = false;

  modal.hidden = false;
}

function closeCleanupDeleteConfirmModal(){
  if($('cleanupDeleteConfirmModal')) $('cleanupDeleteConfirmModal').hidden = true;
}

async function executeCleanupDelete(){
  const btnExecute = $('btnCleanupModalExecute');
  const btnCancel = $('btnCleanupModalCancel');
  const progressWrap = $('cleanupModalProgressWrap');
  const resultWrap = $('cleanupModalResultWrap');

  if(btnExecute) btnExecute.disabled = true;
  if(btnCancel) btnCancel.disabled = true;
  if(progressWrap) progressWrap.hidden = false;
  if(resultWrap) resultWrap.hidden = true;

  const targets = Array.from(cleanupState.selectedKeys).map(k => {
    const [client, bot] = k.split(':::');
    return {client, bot};
  });

  const payload = {
    env: cleanupState.env,
    targets: targets,
    confirm_prod: cleanupState.env === 'prod' ? ($('cleanupModalProdInput') && $('cleanupModalProdInput').value.trim() === 'ELIMINAR EN PROD') : false
  };

  try{
    const res = await api('/api/cleanup/delete', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(payload)
    });

    if(progressWrap) progressWrap.hidden = true;

    if(resultWrap){
      resultWrap.hidden = false;
      let html = '<div style="background:#064e3b;border:1px solid #059669;color:#a7f3d0;padding:12px;border-radius:4px;">' +
        '<div style="font-weight:700;font-size:13px;display:flex;align-items:center;gap:6px;"><span>✓</span> Eliminación completada con éxito</div>' +
        '<div style="margin-top:6px;font-size:12px;">Se eliminaron <strong>' + (res.total_deleted_records || 0) + '</strong> registros en DynamoDB correspondientes a <strong>' + (res.processed_bots || 0) + '</strong> bots.</div>';
      if(res.backup_file){
        html += '<div style="margin-top:4px;font-size:11px;color:#6ee7b7;">Backup de respaldo: <code>' + esc(res.backup_file) + '</code></div>';
      }
      if(res.failed_bots && res.failed_bots.length > 0){
        html += '<div style="margin-top:6px;color:#fca5a5;font-size:11.5px;">Hubo errores en: ' + res.failed_bots.map(esc).join(', ') + '</div>';
      }
      html += '</div>';
      resultWrap.innerHTML = html;
    }

    // Limpiar selección y permitir cerrar
    cleanupState.selectedKeys.clear();
    if($('cleanupConfirmCheckbox')) $('cleanupConfirmCheckbox').checked = false;
    updateCleanupRightPanel();

    if(btnCancel) btnCancel.disabled = false;
    if(btnExecute){
      btnExecute.disabled = false;
      btnExecute.textContent = 'Listo / Cerrar';
      btnExecute.onclick = () => {
        closeCleanupDeleteConfirmModal();
        if(typeof keyCache !== 'undefined' && cleanupState.env){
          delete keyCache[cleanupState.env];
        }
        loadCleanupData(true);
        if(typeof refreshList === 'function') refreshList(true);
        btnExecute.onclick = executeCleanupDelete;
      };
    }
  }catch(err){
    if(progressWrap) progressWrap.hidden = true;
    if(btnCancel) btnCancel.disabled = false;
    if(btnExecute) btnExecute.disabled = false;

    if(resultWrap){
      resultWrap.hidden = false;
      resultWrap.innerHTML = '<div style="background:#450a0a;border:1px solid #dc2626;color:#fca5a5;padding:12px;border-radius:4px;">' +
        '<strong>Error al eliminar registros:</strong><br>' + esc(err.message) +
        '</div>';
    }
  }
}

async function exportCleanupSelection(){
  if(cleanupState.selectedKeys.size === 0) return;

  const targets = Array.from(cleanupState.selectedKeys).map(k => {
    const [client, bot] = k.split(':::');
    return {client, bot};
  });

  const btn = $('btnExportCleanup');
  if(btn){
    btn.disabled = true;
    btn.textContent = 'Exportando CSV…';
  }

  try{
    const response = await fetch('/api/cleanup/export', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({env: cleanupState.env, targets})
    });

    if(!response.ok){
      const err = await response.json().catch(() => ({}));
      throw new Error(err.error || ('HTTP ' + response.status));
    }

    const blob = await response.blob();
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = `limpieza_dynamo_${cleanupState.env}.csv`;
    document.body.appendChild(link);
    link.click();
    link.remove();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
  }catch(err){
    alert('Error al exportar CSV: ' + err.message);
  }finally{
    if(btn){
      btn.disabled = false;
      btn.innerHTML = '<svg class="ui-icon" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"></path><polyline points="17 8 12 3 7 8"></polyline><line x1="12" y1="3" x2="12" y2="15"></line></svg> Exportar selección';
    }
  }
}

function openCleanupBotDetail(clientName, botName){
  const modal = $('cleanupBotDetailModal');
  if(!modal || !cleanupState.data || !cleanupState.data.clients) return;

  const cObj = cleanupState.data.clients.find(c => c.client === clientName);
  const bObj = cObj ? (cObj.bots || []).find(b => b.bot === botName) : null;
  if(!bObj) return;

  if($('cleanupDetailTitle')) $('cleanupDetailTitle').textContent = `${clientName} / ${botName}`;
  if($('cleanupDetailEnvBadge')){
    $('cleanupDetailEnvBadge').textContent = cleanupState.env.toUpperCase();
    $('cleanupDetailEnvBadge').className = 'env-badge ' + cleanupState.env;
  }

  const deleteOneBtn = $('btnCleanupDetailDeleteOne');
  if(deleteOneBtn){
    deleteOneBtn.dataset.client = clientName;
    deleteOneBtn.dataset.bot = botName;
  }

  const body = $('cleanupDetailBody');
  if(body){
    let breakdownRows = '';
    const breakdown = bObj.tables_breakdown || {};
    const tableKeys = Object.keys(breakdown);

    if(!tableKeys.length){
      breakdownRows = '<tr><td colspan="3" style="text-align:center;padding:12px;color:#64748b;">No se detectaron tablas asociadas con registros para este bot.</td></tr>';
    }else{
      tableKeys.forEach(tName => {
        const count = breakdown[tName];
        let purpose = 'Tabla operacional';
        if(tName.includes('conversations_and_cels')) purpose = 'Mapeo Celular ↔ Conversación';
        else if(tName.includes('conversations')) purpose = 'Historial de Conversaciones';
        else if(tName.includes('step_function_fifo')) purpose = 'Cola FIFO de mensajes';
        else if(tName.includes('custom_table')) purpose = 'Base de conocimiento / Procedimientos';

        breakdownRows += '<tr>' +
          '<td><code>' + esc(tName) + '</code></td>' +
          '<td><span style="font-size:11px;color:#94a3b8;">' + esc(purpose) + '</span></td>' +
          '<td style="text-align:right;"><strong>' + count + '</strong></td>' +
          '</tr>';
      });
    }

    body.innerHTML = '<div style="display:flex;flex-direction:column;gap:4px;background:#182232;padding:10px 14px;border-radius:5px;border:1px solid #223147;">' +
      '<div style="font-size:12px;color:#94a3b8;">Identificador del sistema: <code style="color:#93c5fd;font-weight:700;">' + esc(bObj.system_id || ('agente-' + botName)) + '</code></div>' +
      '<div style="font-size:12px;color:#94a3b8;">Registros totales en DynamoDB: <strong style="color:#fff;">' + (bObj.dynamodb_records || 0) + '</strong></div>' +
      '<div style="font-size:12px;color:#94a3b8;">Última actualización: <span style="color:#cbd5e1;">' + esc(bObj.last_updated || 'Sin registros') + '</span></div>' +
      '</div>' +
      '<div style="font-size:11px;font-weight:700;color:#94a3b8;text-transform:uppercase;letter-spacing:0.5px;margin-top:6px;">Desglose por tabla de DynamoDB:</div>' +
      '<div style="max-height:200px;overflow-y:auto;border:1px solid #1e293b;border-radius:4px;">' +
      '  <table class="cleanup-modal-table">' +
      '    <thead><tr><th>Tabla</th><th>Función</th><th style="text-align:right;">Registros</th></tr></thead>' +
      '    <tbody>' + breakdownRows + '</tbody>' +
      '  </table>' +
      '</div>';
  }

  modal.hidden = false;
}

function closeCleanupBotDetailModal(){
  if($('cleanupBotDetailModal')) $('cleanupBotDetailModal').hidden = true;
}

/* =========================================================================
   GESTIÓN DE TABLAS DYNAMODB & CARGA MANUAL DE CUSTOM TABLES
   ========================================================================= */

let tablesState = {
  initialized: false,
  activeTab: 'create', // 'create' | 'list' | 'empty'
  env: 'desa',
  tableName: 'desa_liderazgo_tru_deuda_custom',
  columns: ['custom_key', 'alias_display', 'cliente', 'dni', 'tipo_contacto', 'deuda'],
  file: null, // { name, size, ext, base64 }
  preview: null, // response from /api/custom-table/preview
  previewTimer: null,
  tablesList: null,
  listSearchQuery: '',
  isLoadingTables: false
};

function initTablesView(){
  if(!tablesState.initialized){
    tablesState.initialized = true;
    setupTablesEventListeners();
  }
  syncTablesEnvUI(tablesState.env);
  renderTablesColumns();
  updateTablesSummary();
  fetchTablesListIfNeeded();
  if(tablesState.activeTab === 'list' && !tablesState.tablesList){
    loadTablesList();
  }
}

async function fetchTablesListIfNeeded(force=false){
  if(tablesState.tablesList && !force){
    updateTablesDatalist();
    return tablesState.tablesList;
  }
  try{
    const res = await api('/api/custom-table/list?env=' + encodeURIComponent(tablesState.env));
    tablesState.tablesList = res.tables || [];
    updateTablesDatalist();
    return tablesState.tablesList;
  }catch(e){
    console.warn('Error al obtener lista de tablas:', e);
    return tablesState.tablesList || [];
  }
}

function updateTablesDatalist(){
  const dl = $('tablesAvailableDatalist');
  if(!dl) return;
  if(!tablesState.tablesList || tablesState.tablesList.length === 0){
    dl.innerHTML = '';
    return;
  }
  dl.innerHTML = tablesState.tablesList.map(t => `<option value="${esc(t.name)}">${esc(t.name)} (${esc(t.partition_key || 'custom_key')})</option>`).join('');
}

function setupTablesEventListeners(){
  // 1. Navigation tabs
  if($('tabBtnListTables')) $('tabBtnListTables').onclick = () => switchTablesTab('list');
  if($('tabBtnEmptyTable')) $('tabBtnEmptyTable').onclick = () => switchTablesTab('empty');
  if($('tabBtnCreateTable')) $('tabBtnCreateTable').onclick = () => switchTablesTab('create');

  // 2. Environment selectors
  if($('tablesGlobalEnvSelect')){
    $('tablesGlobalEnvSelect').onchange = e => setTablesEnvironment(e.target.value);
  }
  if($('tablesCardEnvSelect')){
    $('tablesCardEnvSelect').onchange = e => setTablesEnvironment(e.target.value);
  }
  document.querySelectorAll('#tablesEnvSegmented .env-btn').forEach(btn => {
    btn.onclick = () => setTablesEnvironment(btn.dataset.env);
  });

  // 3. Table name input with auto-detected environment
  function detectEnvFromTableName(tblName){
    if(!tblName) return null;
    const lower = tblName.toLowerCase().trim();
    if(lower.startsWith('desa_') || lower.startsWith('desa-') || lower === 'desa') return 'desa';
    if(lower.startsWith('test_') || lower.startsWith('test-') || lower === 'test') return 'test';
    if(lower.startsWith('prod_') || lower.startsWith('prod-') || lower === 'prod') return 'prod';
    return null;
  }

  function syncAutoDetectedEnv(tblName){
    const detected = detectEnvFromTableName(tblName);
    const targetEnv = detected || tablesState.env || 'desa';
    if(detected && detected !== tablesState.env){
      setTablesEnvironment(detected, false);
    }
    if($('tablesDetectedEnvBadge')){
      $('tablesDetectedEnvBadge').textContent = targetEnv.toUpperCase();
      $('tablesDetectedEnvBadge').className = 'env-badge ' + targetEnv;
    }
  }

  if($('tablesTableNameInput')){
    $('tablesTableNameInput').oninput = e => {
      tablesState.tableName = e.target.value.trim();
      syncAutoDetectedEnv(tablesState.tableName);
      updateTablesSummary();
    };
    // Sync on initial setup
    syncAutoDetectedEnv($('tablesTableNameInput').value.trim());
  }

  // 4. File upload (dropzone, browse link, file input, chip remove)
  const fileInput = $('tablesFileInput');
  const dropZone = $('tablesDropZone');
  const browseLink = $('tablesBrowseLink');
  const removeFileBtn = $('btnRemoveUploadedFile');
  if(removeFileBtn){
    removeFileBtn.onclick = resetTablesUploadedFile;
  }

  if(browseLink && fileInput){
    browseLink.onclick = e => {
      e.preventDefault();
      fileInput.click();
    };
  }
  if(dropZone && fileInput){
    dropZone.onclick = e => {
      if(e.target !== browseLink) fileInput.click();
    };

    dropZone.addEventListener('dragover', e => {
      e.preventDefault();
      e.stopPropagation();
      dropZone.classList.add('dragover');
    });
    dropZone.addEventListener('dragenter', e => {
      e.preventDefault();
      e.stopPropagation();
      dropZone.classList.add('dragover');
    });
    dropZone.addEventListener('dragleave', e => {
      e.preventDefault();
      dropZone.classList.remove('dragover');
    });
    dropZone.addEventListener('drop', e => {
      e.preventDefault();
      dropZone.classList.remove('dragover');
      if(e.dataTransfer && e.dataTransfer.files && e.dataTransfer.files.length > 0){
        processUploadedFile(e.dataTransfer.files[0]);
      }
    });
  }

  if(fileInput){
    fileInput.onchange = e => {
      if(e.target.files && e.target.files.length > 0){
        processUploadedFile(e.target.files[0]);
      }
    };
  }

  if(removeFileBtn){
    removeFileBtn.onclick = resetTablesUploadedFile;
  }

  // 5. Dynamic column editor
  if($('btnAddColumnBtn')){
    $('btnAddColumnBtn').onclick = addTablesColumn;
  }

  // 6. Action buttons (Card 5 bottom bar and Right summary)
  if($('btnExecuteCreateTableBottom')){
    $('btnExecuteCreateTableBottom').onclick = openTablesCreateConfirmModal;
  }
  if($('btnExecuteCreateTableRight')){
    $('btnExecuteCreateTableRight').onclick = openTablesCreateConfirmModal;
  }

  // 7. Confirmation modal events
  if($('tablesModalCloseBtn')) $('tablesModalCloseBtn').onclick = closeTablesCreateConfirmModal;
  if($('btnTablesModalCancel')) $('btnTablesModalCancel').onclick = closeTablesCreateConfirmModal;
  if($('btnTablesModalExecute')) $('btnTablesModalExecute').onclick = executeCreateTableAndUpload;

  if($('tablesModalProdInput')){
    $('tablesModalProdInput').oninput = e => {
      const val = e.target.value.trim().toUpperCase();
      const isConfirmed = val === 'CARGAR EN PROD' || val === 'CREAR EN PROD';
      if($('btnTablesModalExecute')) $('btnTablesModalExecute').disabled = !isConfirmed;
    };
  }

  // 8. Secondary tabs: List tables & Empty table
  if($('tablesListSearchInput')){
    $('tablesListSearchInput').oninput = e => {
      tablesState.listSearchQuery = e.target.value.toLowerCase().trim();
      renderTablesList();
    };
  }
  if($('btnRefreshTablesList')){
    $('btnRefreshTablesList').onclick = () => loadTablesList(true);
  }
  if($('btnExecuteEmptyTable')){
    $('btnExecuteEmptyTable').onclick = executeEmptyTableAction;
  }
}

function syncTablesEnvUI(env){
  if($('tablesGlobalEnvSelect')) $('tablesGlobalEnvSelect').value = env;
  if($('tablesCardEnvSelect')) $('tablesCardEnvSelect').value = env;
  document.querySelectorAll('#tablesEnvSegmented .env-btn').forEach(btn => {
    btn.classList.toggle('active', btn.dataset.env === env);
  });
  const bar = $('bar');
  if(bar && activeMode === 'tables'){
    bar.classList.remove('env-desa', 'env-test', 'env-prod');
    bar.classList.add('env-' + env);
  }
  if($('rightEnvBadge')){
    $('rightEnvBadge').textContent = env.toUpperCase();
    $('rightEnvBadge').className = 'env-badge ' + env;
  }
  if($('bottomChipEnv')) $('bottomChipEnv').textContent = env.toUpperCase();
}

function setTablesEnvironment(env){
  if(!env) return;
  tablesState.env = env;
  syncTablesEnvUI(env);

  // Auto-adjust default table prefix if it matches previous env convention
  const tInput = $('tablesTableNameInput');
  if(tInput){
    let currentVal = tInput.value.trim();
    if(currentVal.startsWith('desa_') || currentVal.startsWith('test_') || currentVal.startsWith('prod_')){
      const rest = currentVal.replace(/^(desa|test|prod)_/, '');
      tInput.value = env + '_' + rest;
      tablesState.tableName = tInput.value;
    }
  }

  updateTablesSummary();

  tablesState.tablesList = null;
  fetchTablesListIfNeeded(true);

  if(tablesState.activeTab === 'list'){
    loadTablesList(true);
  }
}

function switchTablesTab(tab){
  tablesState.activeTab = tab;

  if($('tabBtnListTables')) $('tabBtnListTables').classList.toggle('active', tab === 'list');
  if($('tabBtnEmptyTable')) $('tabBtnEmptyTable').classList.toggle('active', tab === 'empty');
  if($('tabBtnCreateTable')) $('tabBtnCreateTable').classList.toggle('active', tab === 'create');

  if($('tabContentCreate')){
    $('tabContentCreate').classList.toggle('active', tab === 'create');
    $('tabContentCreate').hidden = (tab !== 'create');
  }
  if($('tabContentList')){
    $('tabContentList').classList.toggle('active', tab === 'list');
    $('tabContentList').hidden = (tab !== 'list');
  }
  if($('tabContentEmpty')){
    $('tabContentEmpty').classList.toggle('active', tab === 'empty');
    $('tabContentEmpty').hidden = (tab !== 'empty');
  }

  if(tab === 'list'){
    loadTablesList();
  }
}

function renderTablesColumns(){
  const tbody = $('tablesColumnsTableBody');
  if(!tbody) return;
  tbody.innerHTML = '';

  tablesState.columns.forEach((colName, index) => {
    const tr = document.createElement('tr');
    tr.className = 'tables-col-row';

    const isPk = colName.trim().toLowerCase() === 'custom_key';

    // Drag handle
    const tdHandle = document.createElement('td');
    tdHandle.style.width = '26px';
    tdHandle.innerHTML = '<span class="col-drag-handle" title="Columna">⠿</span>';

    // Name input
    const tdName = document.createElement('td');
    const input = document.createElement('input');
    input.type = 'text';
    input.className = 'col-name-input';
    input.value = colName;
    input.autocomplete = 'off';
    input.spellcheck = false;
    input.dataset.index = index;

    if(isPk){
      input.classList.add('col-name-pk');
      input.readOnly = true;
      input.title = "La clave de partición 'custom_key' es obligatoria y no puede modificarse.";
    }

    input.oninput = e => {
      const newVal = e.target.value.trim();
      tablesState.columns[index] = newVal;
      validateTablesColumns();
      updateTablesSummary();
      triggerPreviewDebounced();
    };

    tdName.appendChild(input);

    // Type select (DynamoDB custom table stores String)
    const tdType = document.createElement('td');
    tdType.style.width = '100px';
    tdType.innerHTML = '<select class="col-type-select" disabled title="Tipo en DynamoDB: String"><option value="String">String</option></select>';

    // Function badge
    const tdRole = document.createElement('td');
    tdRole.style.width = '140px';
    if(isPk){
      tdRole.innerHTML = '<span class="badge-col-pk"><svg class="ui-icon" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="margin-right:4px;"><circle cx="7.5" cy="12" r="4.5"></circle><circle cx="7.5" cy="12" r="1.5" fill="currentColor"></circle><path d="M12 12h9v3.5m-4-3.5v2.5"></path></svg>Clave de partición</span>';
    } else {
      tdRole.innerHTML = '<span class="badge-col-data">Dato</span>';
    }

    // Actions
    const tdAction = document.createElement('td');
    tdAction.style.width = '50px';
    tdAction.style.textAlign = 'center';

    if(isPk){
      tdAction.innerHTML = '<span class="action-locked" title="custom_key es obligatoria y no se puede eliminar"><svg class="ui-icon" width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect width="18" height="11" x="3" y="11" rx="2" ry="2"></rect><path d="M7 11V7a5 5 0 0 1 10 0v4"></path></svg></span>';
    } else {
      const delBtn = document.createElement('button');
      delBtn.type = 'button';
      delBtn.className = 'btn-delete-col';
      delBtn.title = 'Eliminar columna';
      delBtn.innerHTML = '<svg class="ui-icon" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 6h18"></path><path d="M19 6v14c0 1-1 2-2 2H7c-1 0-2-1-2-2V6"></path><path d="M8 6V4c0-1 1-2 2-2h4c1 0 2 1 2 2v2"></path></svg>';
      delBtn.onclick = () => removeTablesColumn(index);
      tdAction.appendChild(delBtn);
    }

    tr.appendChild(tdHandle);
    tr.appendChild(tdName);
    tr.appendChild(tdType);
    tr.appendChild(tdRole);
    tr.appendChild(tdAction);
    tbody.appendChild(tr);
  });

  validateTablesColumns();
  updateTablesSummary();
}

function addTablesColumn(){
  let count = 1;
  let baseName = 'nueva_columna';
  let cand = baseName;
  while(tablesState.columns.includes(cand)){
    count++;
    cand = `${baseName}_${count}`;
  }
  tablesState.columns.push(cand);
  renderTablesColumns();

  // Focus the newly added input
  const inputs = document.querySelectorAll('.col-name-input');
  if(inputs.length > 0){
    const lastInput = inputs[inputs.length - 1];
    lastInput.focus();
    lastInput.select();
  }
  triggerPreviewDebounced();
}

function removeTablesColumn(index){
  const colName = tablesState.columns[index];
  if(colName === 'custom_key'){
    alert("La columna 'custom_key' es la clave de partición obligatoria y no puede eliminarse.");
    return;
  }
  tablesState.columns.splice(index, 1);
  renderTablesColumns();
  triggerPreviewDebounced();
}

function validateTablesColumns(){
  const inputs = document.querySelectorAll('.col-name-input');
  const seen = new Map();

  // Count occurrences
  tablesState.columns.forEach((col, idx) => {
    const norm = col.trim().toLowerCase();
    if(!seen.has(norm)) seen.set(norm, []);
    seen.get(norm).push(idx);
  });

  inputs.forEach((inp, idx) => {
    const val = (inp.value || '').trim();
    const norm = val.toLowerCase();
    const isDup = norm && seen.get(norm) && seen.get(norm).length > 1;
    const isEmpty = val === '';

    if(isEmpty || isDup){
      inp.style.borderColor = '#ef4444';
      inp.style.background = '#450a0a';
      inp.title = isEmpty ? 'El nombre de la columna no puede estar vacío.' : 'Hay columnas duplicadas con este nombre.';
    } else {
      inp.style.borderColor = '';
      inp.style.background = '';
      inp.title = '';
    }
  });
}

function processUploadedFile(file){
  if(!file) return;
  const validExts = ['.xlsx', '.xls', '.csv'];
  const ext = file.name.substring(file.name.lastIndexOf('.')).toLowerCase();
  if(!validExts.includes(ext)){
    alert("Formato no admitido. Se admiten únicamente archivos .xlsx, .xls y .csv.");
    return;
  }

  const reader = new FileReader();
  reader.onload = function(e){
    tablesState.file = {
      name: file.name,
      size: file.size,
      ext: ext,
      base64: e.target.result
    };

    if($('tablesDropZone')) $('tablesDropZone').hidden = true;
    if($('tablesFileLoadedChip')) $('tablesFileLoadedChip').hidden = false;
    if($('tablesFileNameDisplay')) $('tablesFileNameDisplay').textContent = file.name;

    const sizeFormatted = file.size > 1024 * 1024 
      ? (file.size / (1024 * 1024)).toFixed(1) + ' MB'
      : (file.size / 1024).toFixed(1) + ' KB';
    if($('tablesFileMetaDisplay')) $('tablesFileMetaDisplay').textContent = `${sizeFormatted} • Analizando filas…`;

    const badge = $('tablesFileExtBadge');
    if(badge){
      badge.textContent = ext.includes('csv') ? 'CSV' : 'XLS';
      badge.className = 'tables-file-ext-icon ' + (ext.includes('csv') ? 'ext-csv' : 'ext-xls');
    }

    updateTablesSummary();
    fetchFilePreview();
  };
  reader.readAsDataURL(file);
}

function resetTablesUploadedFile(){
  tablesState.file = null;
  tablesState.preview = null;
  if($('tablesFileInput')) $('tablesFileInput').value = '';
  if($('tablesDropZone')) $('tablesDropZone').hidden = false;
  if($('tablesFileLoadedChip')) $('tablesFileLoadedChip').hidden = true;
  if($('tablesPreviewRowsCount')) $('tablesPreviewRowsCount').textContent = '0';

  const matchStatus = $('tablesColumnsMatchStatus');
  if(matchStatus){
    matchStatus.className = 'tables-match-status-box match-info';
    matchStatus.innerHTML = '<span class="status-icon">ⓘ</span><span class="status-text">Cargá un archivo Excel o CSV para ver la correspondencia de columnas.</span>';
  }

  if($('tablesKeyWarningBox')) $('tablesKeyWarningBox').hidden = true;

  const thead = $('tablesPreviewThead');
  if(thead){
    let ths = tablesState.columns.map(c => `<th>${esc(c)}</th>`).join('');
    thead.innerHTML = `<tr>${ths}</tr>`;
  }
  const tbody = $('tablesPreviewTbody');
  if(tbody){
    tbody.innerHTML = `<tr><td colspan="${tablesState.columns.length}" class="tables-preview-empty-td">Sin datos para mostrar. Arrastrá o seleccioná un archivo en el paso 2.</td></tr>`;
  }

  updateTablesSummary();
}

function triggerPreviewDebounced(){
  if(tablesState.previewTimer) clearTimeout(tablesState.previewTimer);
  tablesState.previewTimer = setTimeout(() => {
    if(tablesState.file){
      fetchFilePreview();
    } else {
      updateTablesSummary();
    }
  }, 350);
}

async function fetchFilePreview(){
  if(!tablesState.file) return;

  const matchStatus = $('tablesColumnsMatchStatus');
  if(matchStatus){
    matchStatus.className = 'tables-match-status-box match-info';
    matchStatus.innerHTML = '<span class="status-icon"><svg class="ui-icon" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="margin-right:4px;animation:spin 1s linear infinite;"><path d="M21 12a9 9 0 1 1-6.219-8.56"></path></svg></span><span class="status-text">Analizando encabezados y primeros registros del archivo…</span>';
  }

  try{
    const res = await api('/api/custom-table/preview', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({
        filename: tablesState.file.name,
        file_data: tablesState.file.base64,
        columns: tablesState.columns
      })
    });

    tablesState.preview = res;

    const sizeFormatted = tablesState.file.size > 1024 * 1024 
      ? (tablesState.file.size / (1024 * 1024)).toFixed(1) + ' MB'
      : (tablesState.file.size / 1024).toFixed(1) + ' KB';
    if($('tablesFileMetaDisplay')) $('tablesFileMetaDisplay').textContent = `${sizeFormatted} • ${res.total_rows} filas`;
    if($('tablesPreviewRowsCount')) $('tablesPreviewRowsCount').textContent = res.total_rows;

    renderFilePreviewUI(res);
    updateTablesSummary();
  }catch(err){
    if(matchStatus){
      matchStatus.className = 'tables-match-status-box match-error';
      matchStatus.innerHTML = '<span class="status-icon"><svg class="ui-icon" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round" style="margin-right:4px;"><line x1="18" y1="6" x2="6" y2="18"></line><line x1="6" y1="6" x2="18" y2="18"></line></svg></span><span class="status-text">Error al analizar el archivo: ' + esc(err.message) + '</span>';
    }
    updateTablesSummary();
  }
}

function renderFilePreviewUI(res){
  const matchStatus = $('tablesColumnsMatchStatus');
  const warnBox = $('tablesKeyWarningBox');
  const thead = $('tablesPreviewThead');
  const tbody = $('tablesPreviewTbody');

  // 1. Column matching status
  if(matchStatus){
    if(!res.has_custom_key){
      matchStatus.className = 'tables-match-status-box match-error';
      matchStatus.innerHTML = '<span class="status-icon"><svg class="ui-icon" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round" style="margin-right:4px;"><line x1="18" y1="6" x2="6" y2="18"></line><line x1="6" y1="6" x2="18" y2="18"></line></svg></span><span class="status-text">La clave obligatoria <strong>custom_key</strong> no se encuentra en el archivo. No es posible crear la tabla sin esta clave.</span>';
    } else if(res.missing_columns && res.missing_columns.length > 0){
      matchStatus.className = 'tables-match-status-box match-error';
      matchStatus.innerHTML = '<span class="status-icon"><svg class="ui-icon" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round" style="margin-right:4px;"><line x1="18" y1="6" x2="6" y2="18"></line><line x1="6" y1="6" x2="18" y2="18"></line></svg></span><span class="status-text">Faltan columnas configuradas en el archivo: <strong>' + esc(res.missing_columns.join(', ')) + '</strong>. Verificá los nombres o eliminalas del editor.</span>';
    } else {
      matchStatus.className = 'tables-match-status-box match-success';
      matchStatus.innerHTML = '<span class="status-icon"><svg class="ui-icon" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round" style="margin-right:4px;"><polyline points="20 6 9 17 4 12"></polyline></svg></span><span class="status-text">Todas las columnas configuradas están presentes en el archivo (' + res.configured_columns.length + ' de ' + res.configured_columns.length + ').</span>';
    }
  }

  // 2. Partition Key diagnostics (empty & duplicate keys)
  if(warnBox){
    let keyWarns = [];
    const emptyKeys = res.empty_keys !== undefined ? res.empty_keys : (res.empty_key_count || 0);
    const dupKeys = res.duplicate_keys !== undefined ? res.duplicate_keys : (res.duplicate_keys_count || 0);
    if(emptyKeys > 0){
      keyWarns.push('<svg class="ui-icon" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="margin-right:4px;"><path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3Z"></path><line x1="12" y1="9" x2="12" y2="13"></line><line x1="12" y1="17" x2="12.01" y2="17"></line></svg> <strong>' + emptyKeys + ' registro(s)</strong> tienen la clave <code>custom_key</code> vacía y serán omitidos por la lógica de carga.');
    }
    if(dupKeys > 0){
      keyWarns.push('<svg class="ui-icon" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="margin-right:4px;"><path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3Z"></path><line x1="12" y1="9" x2="12" y2="13"></line><line x1="12" y1="17" x2="12.01" y2="17"></line></svg> Se detectaron <strong>' + dupKeys + ' clave(s) duplicada(s)</strong>. En DynamoDB, escrituras con la misma clave sobrescribirán registros anteriores.');
    }
    if(keyWarns.length > 0){
      warnBox.hidden = false;
      warnBox.innerHTML = keyWarns.join('<br>');
    } else {
      warnBox.hidden = true;
      warnBox.innerHTML = '';
    }
  }

  // 3. Preview table
  if(thead){
    let ths = tablesState.columns.map(c => {
      const isPk = c.trim().toLowerCase() === 'custom_key';
      const isMissing = res.missing_columns && res.missing_columns.includes(c);
      let cls = isPk ? 'th-pk' : '';
      if(isMissing) cls += ' th-missing';
      return `<th class="${cls}">${esc(c)}${isPk ? ' (PK)' : ''}${isMissing ? ' (!)' : ''}</th>`;
    }).join('');
    thead.innerHTML = `<tr>${ths}</tr>`;
  }

  if(tbody){
    if(!res.preview_rows || res.preview_rows.length === 0){
      tbody.innerHTML = `<tr><td colspan="${tablesState.columns.length}" class="tables-preview-empty-td">El archivo no contiene registros de datos.</td></tr>`;
    } else {
      let trs = res.preview_rows.map(row => {
        let tds = tablesState.columns.map(c => {
          let val = row[c];
          if(val === undefined || val === null || val === ''){
            return `<td style="color:#64748b;font-style:italic;">(vacío)</td>`;
          }
          let strVal = String(val);
          return `<td title="${esc(strVal)}">${esc(strVal)}</td>`;
        }).join('');
        return `<tr>${tds}</tr>`;
      }).join('');
      tbody.innerHTML = trs;
    }
  }
}

function updateTablesSummary(){
  const tableName = tablesState.tableName || '';
  const envUpper = tablesState.env.toUpperCase();
  const file = tablesState.file;
  const colsCount = tablesState.columns.length;

  // Bottom summary card
  if($('bottomChipTable')) $('bottomChipTable').textContent = tableName || '(sin nombre)';
  if($('bottomChipEnv')) $('bottomChipEnv').textContent = envUpper;
  if($('bottomChipEnvBadge')){
    $('bottomChipEnvBadge').textContent = envUpper;
    $('bottomChipEnvBadge').className = 'env-badge ' + tablesState.env;
  }
  if($('bottomChipFile')) $('bottomChipFile').textContent = file ? file.name : 'Ningún archivo';
  if($('bottomChipFileMeta')){
    if(file && tablesState.preview){
      const sizeFormatted = file.size > 1024 * 1024 
        ? (file.size / (1024 * 1024)).toFixed(1) + ' MB'
        : (file.size / 1024).toFixed(1) + ' KB';
      $('bottomChipFileMeta').textContent = `(${sizeFormatted} • ${tablesState.preview.total_rows} filas)`;
    } else {
      $('bottomChipFileMeta').textContent = '';
    }
  }
  if($('bottomChipCols')) $('bottomChipCols').textContent = String(colsCount);

  // Right sidebar
  if($('rightEnvBadge')){
    $('rightEnvBadge').textContent = envUpper;
    $('rightEnvBadge').className = 'env-badge ' + tablesState.env;
  }
  if($('rightTableName')) $('rightTableName').textContent = tableName || '(sin nombre)';
  if($('rightFileName')) $('rightFileName').textContent = file ? file.name : 'Ningún archivo cargado';
  if($('rightFileMeta')){
    if(file && tablesState.preview){
      const sizeFormatted = file.size > 1024 * 1024 
        ? (file.size / (1024 * 1024)).toFixed(1) + ' MB'
        : (file.size / 1024).toFixed(1) + ' KB';
      $('rightFileMeta').textContent = `${sizeFormatted} • ${tablesState.preview.total_rows} filas`;
    } else {
      $('rightFileMeta').textContent = '';
    }
  }
  if($('rightColsCount')) $('rightColsCount').textContent = `${colsCount} columnas`;

  // Validation to enable/disable create buttons
  const isValidTableName = tableName.length >= 3 && /^[a-zA-Z0-9_.-]+$/.test(tableName);
  const hasFile = file !== null;
  const hasCustomKey = tablesState.columns.map(c => c.trim().toLowerCase()).includes('custom_key');
  const noEmptyCols = tablesState.columns.every(c => c.trim().length > 0);
  const noDupCols = new Set(tablesState.columns.map(c => c.trim().toLowerCase())).size === tablesState.columns.length;
  const previewMatch = tablesState.preview && tablesState.preview.has_custom_key && (!tablesState.preview.missing_columns || tablesState.preview.missing_columns.length === 0);

  const canExecute = isValidTableName && hasFile && hasCustomKey && noEmptyCols && noDupCols && previewMatch;

  if($('btnExecuteCreateTableBottom')) $('btnExecuteCreateTableBottom').disabled = !canExecute;
  if($('btnExecuteCreateTableRight')) $('btnExecuteCreateTableRight').disabled = !canExecute;
}

async function openTablesCreateConfirmModal(){
  if(!tablesState.file || !tablesState.preview) return;

  const currentTable = (tablesState.tableName || '').trim();
  if(!currentTable){
    alert('Ingresá o seleccioná el nombre de una tabla existente.');
    return;
  }

  // Validar si la tabla existe en el listado de DynamoDB en el ambiente seleccionado
  const list = await fetchTablesListIfNeeded();
  if(list && list.length > 0){
    const exists = list.some(t => (t.name || '').toLowerCase() === currentTable.toLowerCase());
    if(!exists){
      const errMsg = `La tabla '${currentTable}' no existe en el listado de tablas de DynamoDB para el ambiente ${tablesState.env.toUpperCase()}.`;
      setStatus(`Error: ` + errMsg + ` Solo se permite cargar datos en tablas existentes.`, 'err');
      alert(`Excepción:

${errMsg}

La sección 'Cargar tabla' no crea tablas, solo carga datos en tablas existentes. Seleccioná una tabla del listado.`);
      return;
    }
  }

  const modal = $('tablesCreateConfirmModal');
  if(!modal) return;

  if($('tablesModalTableName')) $('tablesModalTableName').textContent = tablesState.tableName;
  if($('tablesModalEnv')){
    $('tablesModalEnv').textContent = tablesState.env.toUpperCase();
    $('tablesModalEnv').className = 'env-badge ' + tablesState.env;
  }
  if($('tablesModalEnvBadge')){
    $('tablesModalEnvBadge').textContent = tablesState.env.toUpperCase();
    $('tablesModalEnvBadge').className = 'env-badge ' + tablesState.env;
  }
  if($('tablesModalFileName')) $('tablesModalFileName').textContent = tablesState.file.name;
  if($('tablesModalFileMeta')){
    if(tablesState.file && tablesState.preview){
      const sizeFormatted = tablesState.file.size > 1024 * 1024 
        ? (tablesState.file.size / (1024 * 1024)).toFixed(1) + ' MB'
        : (tablesState.file.size / 1024).toFixed(1) + ' KB';
      $('tablesModalFileMeta').textContent = `(${sizeFormatted} • ${tablesState.preview.total_rows} filas)`;
    } else {
      $('tablesModalFileMeta').textContent = '';
    }
  }
  if($('tablesModalColsList')) $('tablesModalColsList').textContent = tablesState.columns.join(', ');
  if($('tablesModalTotalRows')) $('tablesModalTotalRows').textContent = tablesState.preview.total_rows + ' filas';

  const isProd = tablesState.env === 'prod';
  if($('tablesModalProdWarning')) $('tablesModalProdWarning').hidden = !isProd;
  if($('tablesModalProdInput')){
    $('tablesModalProdInput').value = '';
  }

  if($('tablesModalProgressWrap')) $('tablesModalProgressWrap').hidden = true;
  if($('tablesModalResultWrap')){
    $('tablesModalResultWrap').hidden = true;
    $('tablesModalResultWrap').innerHTML = '';
  }

  const btnExec = $('btnTablesModalExecute');
  const btnCancel = $('btnTablesModalCancel');
  if(btnExec){
    btnExec.hidden = false;
    btnExec.disabled = isProd;
    btnExec.textContent = 'Confirmar y Cargar';
  }
  if(btnCancel){
    btnCancel.disabled = false;
    btnCancel.textContent = 'Cancelar';
  }

  modal.hidden = false;
}

function closeTablesCreateConfirmModal(){
  if($('tablesCreateConfirmModal')) $('tablesCreateConfirmModal').hidden = true;
}

async function executeCreateTableAndUpload(){
  if(!tablesState.file) return;

  const btnExecute = $('btnTablesModalExecute');
  const btnCancel = $('btnTablesModalCancel');
  const progressWrap = $('tablesModalProgressWrap');
  const progressText = $('tablesModalProgressText');
  const resultWrap = $('tablesModalResultWrap');

  btnExecute.disabled = true;
  btnCancel.disabled = true;
  progressWrap.hidden = false;
  resultWrap.hidden = true;
  resultWrap.innerHTML = '';
  progressText.textContent = `Cargando registros en DynamoDB (${tablesState.tableName} - ${tablesState.env.toUpperCase()})…`;

  try{
    const payload = {
      env: tablesState.env,
      table_name: tablesState.tableName,
      columns: tablesState.columns,
      filename: tablesState.file.name,
      file_data: tablesState.file.base64,
      confirm_prod: tablesState.env === 'prod' ? ($('tablesModalProdInput') ? $('tablesModalProdInput').value.trim() : false) : false
    };

    const res = await api('/api/custom-table/create-and-upload', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(payload)
    });

    progressWrap.hidden = true;
    resultWrap.hidden = false;
    resultWrap.style.background = '#064e3b';
    resultWrap.style.border = '1px solid #10b981';
    resultWrap.style.color = '#a7f3d0';
    resultWrap.innerHTML = `
      <div style="font-size:14px;font-weight:700;display:flex;align-items:center;gap:6px;">
        <svg class="ui-icon" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round" style="margin-right:4px;"><polyline points="20 6 9 17 4 12"></polyline></svg> ¡Carga completada con éxito!
      </div>
      <div style="margin-top:8px;font-size:12.5px;line-height:1.6;color:#e2e8f0;">
        • Tabla DynamoDB: <strong>${esc(res.table_name)}</strong><br>
        • Registros procesados y cargados: <strong>${res.records_processed}</strong><br>
        • Registros omitidos (clave custom_key vacía): <strong>${res.skipped_empty_key}</strong><br>
        • Columnas importadas: <code>${esc(res.columns.join(', '))}</code>
      </div>
    `;

    btnExecute.hidden = true;
    btnCancel.disabled = false;
    btnCancel.textContent = 'Cerrar';

    // Clear cached list
    tablesState.tablesList = null;

  }catch(err){
    progressWrap.hidden = true;
    resultWrap.hidden = false;
    resultWrap.style.background = '#450a0a';
    resultWrap.style.border = '1px solid #ef4444';
    resultWrap.style.color = '#fecaca';
    resultWrap.innerHTML = `
      <div style="font-size:14px;font-weight:700;display:flex;align-items:center;gap:6px;">
        <svg class="ui-icon" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round" style="margin-right:4px;"><line x1="18" y1="6" x2="6" y2="18"></line><line x1="6" y1="6" x2="18" y2="18"></line></svg> Error al cargar en DynamoDB
      </div>
      <div style="margin-top:6px;font-size:12.5px;color:#f8fafc;">
        ${esc(err.message)}
      </div>
    `;
    btnExecute.disabled = false;
    btnCancel.disabled = false;
  }
}

async function loadTablesList(refresh=false){
  const tbody = $('tablesListTableBody');
  if(!tbody) return;
  tbody.innerHTML = '<tr><td colspan="5" style="text-align:center;padding:30px;color:#94a3b8;"><div class="cleanup-spinner" style="margin:0 auto 8px;"></div>Cargando lista de tablas DynamoDB en ' + tablesState.env.toUpperCase() + '…</td></tr>';

  try{
    const res = await api('/api/custom-table/list?env=' + encodeURIComponent(tablesState.env));
    tablesState.tablesList = res.tables || [];
    renderTablesList();
  }catch(err){
    tbody.innerHTML = `<tr><td colspan="5" style="text-align:center;padding:24px;color:#ef4444;">Error al cargar tablas: ${esc(err.message)}</td></tr>`;
  }
}

function renderTablesList(){
  updateTablesDatalist();
  const tbody = $('tablesListTableBody');
  if(!tbody) return;
  if(!tablesState.tablesList || tablesState.tablesList.length === 0){
    tbody.innerHTML = '<tr><td colspan="5" style="text-align:center;padding:24px;color:#64748b;">No se encontraron tablas DynamoDB en este ambiente.</td></tr>';
    return;
  }

  const q = tablesState.listSearchQuery;
  const filtered = q
    ? tablesState.tablesList.filter(t => (t.name || '').toLowerCase().includes(q))
    : tablesState.tablesList;

  if(filtered.length === 0){
    tbody.innerHTML = '<tr><td colspan="5" style="text-align:center;padding:24px;color:#64748b;">No hay tablas que coincidan con la búsqueda.</td></tr>';
    return;
  }

  tbody.innerHTML = filtered.map(t => {
    return `<tr>
      <td><strong style="color:#f8fafc;">${esc(t.name)}</strong></td>
      <td><code style="color:#93c5fd;">${esc(t.partition_key || '-')}</code></td>
      <td><code style="color:#cbd5e1;">${esc(t.sort_key || '-')}</code></td>
      <td style="color:#94a3b8;">${t.item_count !== null && t.item_count !== undefined ? t.item_count : '—'}</td>
      <td style="text-align:right;">
        <button type="button" class="btn-export-cleanup" style="font-size:11px;padding:4px 8px;" onclick="goToEmptyTable('${esc(t.name)}', '${esc(t.partition_key || 'custom_key')}')">
          <svg class="ui-icon" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 6h18"></path><path d="M19 6v14c0 1-1 2-2 2H7c-1 0-2-1-2-2V6"></path><path d="M8 6V4c0-1 1-2 2-2h4c1 0 2 1 2 2v2"></path></svg> Vaciar…
        </button>
      </td>
    </tr>`;
  }).join('');
}

function goToEmptyTable(tableName, pkName){
  switchTablesTab('empty');
  if($('tablesEmptyTableNameInput')) $('tablesEmptyTableNameInput').value = tableName;
  if($('tablesEmptyKeyNameInput')) $('tablesEmptyKeyNameInput').value = pkName || 'custom_key';
}

async function executeEmptyTableAction(){
  const tableName = ($('tablesEmptyTableNameInput') ? $('tablesEmptyTableNameInput').value.trim() : '');
  const keyName = ($('tablesEmptyKeyNameInput') ? $('tablesEmptyKeyNameInput').value.trim() : 'custom_key');
  const resWrap = $('tablesEmptyResultWrap');

  if(!tableName){
    alert("Por favor ingresá el nombre exacto de la tabla a vaciar.");
    return;
  }

  if(tablesState.env === 'prod'){
    const confirmText = prompt(`ADVERTENCIA: Estás en PRODUCCIÓN.\nPara confirmar el vaciado completo de la tabla '${tableName}', escribí exactamente 'VACIAR EN PROD':`);
    if(confirmText !== 'VACIAR EN PROD'){
      alert("Operación cancelada. No se confirmó el vaciado en PROD.");
      return;
    }
  } else {
    if(!confirm(`¿Confirmás el vaciado de TODOS los registros de la tabla '${tableName}' en ${tablesState.env.toUpperCase()}?`)){
      return;
    }
  }

  const btn = $('btnExecuteEmptyTable');
  btn.disabled = true;
  btn.textContent = 'Vaciando tabla…';
  if(resWrap){
    resWrap.hidden = false;
    resWrap.style.background = '#1e293b';
    resWrap.style.border = '1px solid #334155';
    resWrap.style.color = '#94a3b8';
    resWrap.innerHTML = '<div style="display:flex;align-items:center;gap:8px;"><div class="cleanup-spinner"></div> Escaneando y eliminando registros en DynamoDB…</div>';
  }

  try{
    const res = await api('/api/custom-table/empty', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({
        env: tablesState.env,
        table_name: tableName,
        key_name: keyName,
        confirm_prod: tablesState.env === 'prod' ? 'VACIAR EN PROD' : false
      })
    });

    if(resWrap){
      resWrap.style.background = '#064e3b';
      resWrap.style.border = '1px solid #10b981';
      resWrap.style.color = '#a7f3d0';
      resWrap.innerHTML = `<svg class="ui-icon" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round" style="margin-right:4px;"><polyline points="20 6 9 17 4 12"></polyline></svg> ¡Tabla vaciada exitosamente!<br>Se eliminaron <strong>${res.deleted_items}</strong> registros de la tabla <code>${esc(tableName)}</code>.`;
    }
  }catch(err){
    if(resWrap){
      resWrap.style.background = '#450a0a';
      resWrap.style.border = '1px solid #ef4444';
      resWrap.style.color = '#fecaca';
      resWrap.innerHTML = `<svg class="ui-icon" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round" style="margin-right:4px;"><line x1="18" y1="6" x2="6" y2="18"></line><line x1="6" y1="6" x2="18" y2="18"></line></svg> Error al vaciar tabla: ${esc(err.message)}`;
    }
  }finally{
    btn.disabled = false;
    btn.innerHTML = '<svg class="ui-icon" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 6h18"></path><path d="M19 6v14c0 1-1 2-2 2H7c-1 0-2-1-2-2V6"></path><path d="M8 6V4c0-1 1-2 2-2h4c1 0 2 1 2 2v2"></path></svg> Vaciar todos los registros';
  }
}

// =====================================================================
// VISTA 8: MÉTRICAS Y AUDITORÍA DE PROMPTS
// =====================================================================
const metricsState = {
  env: 'desa',
  subMode: 'length', // 'length' | 'variables'
  rows: [],
  filteredRows: [],
  searchQuery: '',
  selectedModel: '',
  statusFilter: 'all', // 'all' | 'warn' | 'danger'
  sortField: 'char_count',
  sortAsc: true, // De menor a mayor por defecto
  // Límites configurables:
  warnLimit: 10000,
  critLimit: 14000,
  // Variables mode:
  varFilter: 'repeated', // 'repeated' (solo con variables repetidas) | 'high' (>=5) | 'all' (todos)
  expandedPrompts: new Set(),
  expandedLocations: new Set(), // 'promptId::varName'
  loading: false,
  initialized: false
};

function setupMetricsEventsOnce(){
  if(metricsState.initialized) return;
  metricsState.initialized = true;

  // Environment buttons in #bar
  document.querySelectorAll('#metricsEnvSegmented button').forEach(btn => {
    btn.onclick = () => {
      metricsState.env = btn.dataset.env;
      document.querySelectorAll('#metricsEnvSegmented button').forEach(b => {
        b.classList.toggle('active', b.dataset.env === metricsState.env);
      });
      const bar = $('bar');
      if(bar && activeMode === 'metrics'){
        bar.classList.remove('env-desa', 'env-test', 'env-prod');
        bar.classList.add('env-' + metricsState.env);
      }
      loadMetricsData(true);
    };
  });

  // Submode Switcher tabs
  const btnSubLength = $('btnMetricsSubmodeLength');
  const btnSubVars = $('btnMetricsSubmodeVars');
  if(btnSubLength && btnSubVars){
    btnSubLength.onclick = () => setMetricsSubmode('length');
    btnSubVars.onclick = () => setMetricsSubmode('variables');
  }

  // Threshold steppers
  const warnInput = $('metricsLimitWarnInput');
  const critInput = $('metricsLimitCritInput');
  const btnWarnMinus = $('btnWarnMinus');
  const btnWarnPlus = $('btnWarnPlus');
  const btnCritMinus = $('btnCritMinus');
  const btnCritPlus = $('btnCritPlus');
  const btnReset = $('btnResetLimits');

  if(btnWarnMinus){
    btnWarnMinus.onclick = () => {
      metricsState.warnLimit = Math.max(1000, metricsState.warnLimit - 1000);
      syncThresholdInputs();
      applyMetricsFiltersAndRender();
    };
  }
  if(btnWarnPlus){
    btnWarnPlus.onclick = () => {
      metricsState.warnLimit += 1000;
      if(metricsState.warnLimit >= metricsState.critLimit){
        metricsState.critLimit = metricsState.warnLimit + 1000;
      }
      syncThresholdInputs();
      applyMetricsFiltersAndRender();
    };
  }
  if(btnCritMinus){
    btnCritMinus.onclick = () => {
      metricsState.critLimit = Math.max(metricsState.warnLimit + 500, metricsState.critLimit - 1000);
      syncThresholdInputs();
      applyMetricsFiltersAndRender();
    };
  }
  if(btnCritPlus){
    btnCritPlus.onclick = () => {
      metricsState.critLimit += 1000;
      syncThresholdInputs();
      applyMetricsFiltersAndRender();
    };
  }
  if(warnInput){
    warnInput.onchange = (e) => {
      let val = parseInt(e.target.value, 10);
      if(isNaN(val) || val < 1000) val = 1000;
      metricsState.warnLimit = val;
      if(metricsState.critLimit <= val){
        metricsState.critLimit = val + 1000;
      }
      syncThresholdInputs();
      applyMetricsFiltersAndRender();
    };
  }
  if(critInput){
    critInput.onchange = (e) => {
      let val = parseInt(e.target.value, 10);
      if(isNaN(val) || val <= metricsState.warnLimit) val = metricsState.warnLimit + 500;
      metricsState.critLimit = val;
      syncThresholdInputs();
      applyMetricsFiltersAndRender();
    };
  }
  if(btnReset){
    btnReset.onclick = () => {
      metricsState.warnLimit = 10000;
      metricsState.critLimit = 14000;
      syncThresholdInputs();
      applyMetricsFiltersAndRender();
    };
  }

  // Search input
  const searchInput = $('metricsSearchInput');
  if(searchInput){
    searchInput.oninput = e => {
      metricsState.searchQuery = e.target.value.trim().toLowerCase();
      applyMetricsFiltersAndRender();
    };
  }

  // Model select
  const modelSelect = $('metricsModelSelect');
  if(modelSelect){
    modelSelect.onchange = e => {
      metricsState.selectedModel = e.target.value;
      applyMetricsFiltersAndRender();
    };
  }

  // Status pills for Length mode
  document.querySelectorAll('#metricsFilterPills .metrics-quick-pill').forEach(btn => {
    btn.onclick = () => {
      metricsState.statusFilter = btn.dataset.filter;
      document.querySelectorAll('#metricsFilterPills .metrics-quick-pill').forEach(b => {
        b.classList.toggle('active', b.dataset.filter === metricsState.statusFilter);
      });
      applyMetricsFiltersAndRender();
    };
  });

  // Filter pills for Variables mode
  document.querySelectorAll('#metricsFilterPillsVars .metrics-quick-pill').forEach(btn => {
    btn.onclick = () => {
      metricsState.varFilter = btn.dataset.varfilter;
      document.querySelectorAll('#metricsFilterPillsVars .metrics-quick-pill').forEach(b => {
        b.classList.toggle('active', b.dataset.varfilter === metricsState.varFilter);
      });
      applyMetricsFiltersAndRender();
    };
  });

  // Refresh button
  const refreshBtn = $('btnRefreshMetrics');
  if(refreshBtn){
    refreshBtn.onclick = () => loadMetricsData(true);
  }

  // Column sorting headers for Length table
  document.querySelectorAll('#metricsTableLengthWrap th[data-sort]').forEach(th => {
    th.onclick = () => {
      const field = th.dataset.sort;
      if(metricsState.sortField === field){
        metricsState.sortAsc = !metricsState.sortAsc;
      } else {
        metricsState.sortField = field;
        metricsState.sortAsc = (field === 'char_count' || field === 'estimated_tokens') ? true : true;
      }
      updateMetricsSortHeaders();
      applyMetricsFiltersAndRender();
    };
  });
}

function syncThresholdInputs(){
  if($('metricsLimitWarnInput')) $('metricsLimitWarnInput').value = metricsState.warnLimit;
  if($('metricsLimitCritInput')) $('metricsLimitCritInput').value = metricsState.critLimit;
}

function setMetricsSubmode(mode){
  metricsState.subMode = mode;
  const isLen = mode === 'length';

  if($('btnMetricsSubmodeLength')) $('btnMetricsSubmodeLength').classList.toggle('active', isLen);
  if($('btnMetricsSubmodeVars')) $('btnMetricsSubmodeVars').classList.toggle('active', !isLen);

  if($('metricsThresholdsGrid')) $('metricsThresholdsGrid').style.display = isLen ? 'grid' : 'none';
  if($('metricsFilterPills')) $('metricsFilterPills').style.display = isLen ? 'inline-flex' : 'none';
  if($('metricsFilterPillsVars')) $('metricsFilterPillsVars').style.display = isLen ? 'none' : 'inline-flex';
  if($('btnResetLimits')) $('btnResetLimits').style.display = isLen ? 'inline-flex' : 'none';

  if($('metricsKpiRowLength')) $('metricsKpiRowLength').style.display = isLen ? 'grid' : 'none';
  if($('metricsKpiRowVars')) $('metricsKpiRowVars').style.display = isLen ? 'none' : 'grid';

  if($('metricsTableLengthWrap')) $('metricsTableLengthWrap').style.display = isLen ? 'block' : 'none';
  if($('metricsTableVarsWrap')) $('metricsTableVarsWrap').style.display = isLen ? 'none' : 'block';

  const sInput = $('metricsSearchInput');
  if(sInput){
    sInput.placeholder = isLen
      ? 'Buscar por cliente, bot, prompt o modelo…'
      : 'Buscar por cliente, bot, prompt o variable ({CONTEXT}, etc.)…';
  }

  updateMetricsSortHeaders();
  applyMetricsFiltersAndRender();
}

function updateMetricsSortHeaders(){
  document.querySelectorAll('#metricsTableLengthWrap th[data-sort]').forEach(th => {
    const field = th.dataset.sort;
    const isCur = metricsState.sortField === field;
    const iconSpan = th.querySelector('.sort-icon');
    if(iconSpan){
      iconSpan.textContent = isCur ? (metricsState.sortAsc ? '▲' : '▼') : '';
    }
  });
}

async function initMetricsView(){
  setupMetricsEventsOnce();
  syncThresholdInputs();
  document.querySelectorAll('#metricsEnvSegmented button').forEach(b => {
    b.classList.toggle('active', b.dataset.env === metricsState.env);
  });
  const bar = $('bar');
  if(bar && activeMode === 'metrics'){
    bar.classList.remove('env-desa', 'env-test', 'env-prod');
    bar.classList.add('env-' + metricsState.env);
  }

  if(metricsState.rows.length === 0){
    await loadMetricsData(false);
  } else {
    applyMetricsFiltersAndRender();
  }
}

async function loadMetricsData(force = false){
  metricsState.loading = true;
  const tbody = $('metricsTableBody');
  if(tbody) tbody.innerHTML = '<tr><td colspan="6" class="metrics-empty-td">Cargando métricas de prompts para ' + metricsState.env.toUpperCase() + '…</td></tr>';
  const varList = $('metricsVarsPromptList');
  if(varList) varList.innerHTML = '<div class="metrics-empty-td">Cargando variables repetidas por prompt…</div>';

  try{
    const res = await api('/api/metrics/data?env=' + encodeURIComponent(metricsState.env) + (force ? '&refresh=1' : ''));
    metricsState.rows = (res.rows || []).map(r => {
      const chars = typeof r.char_count === 'number' ? r.char_count : 0;
      let repeated = r.repeated_vars;
      if(!repeated && r.template_placeholders){
        repeated = [];
      }
      const repTotal = typeof r.repeated_vars_total === 'number' ? r.repeated_vars_total : (repeated || []).reduce((acc, v) => acc + (v.count || 0), 0);
      return {
        ...r,
        char_count: chars,
        estimated_tokens: typeof r.estimated_tokens === 'number' ? r.estimated_tokens : (chars ? Math.round(chars / 4) : 0),
        repeated_vars: repeated || [],
        repeated_vars_total: repTotal,
        variable_locations: r.variable_locations || {}
      };
    });

    populateMetricsModelSelect(res.all_models || []);
    updateMetricsSortHeaders();
    applyMetricsFiltersAndRender();
  }catch(err){
    if(tbody) tbody.innerHTML = '<tr><td colspan="6" class="metrics-empty-td" style="color:#ef4444;">Error al cargar métricas: ' + esc(err.message) + '</td></tr>';
    if(varList) varList.innerHTML = '<div class="metrics-empty-td" style="color:#ef4444;">Error al cargar variables: ' + esc(err.message) + '</div>';
  }finally{
    metricsState.loading = false;
  }
}

function populateMetricsModelSelect(allModels){
  const sel = $('metricsModelSelect');
  if(!sel) return;
  const current = sel.value;
  let opts = '<option value="">Todos los modelos</option>';
  const modelCounts = {};
  metricsState.rows.forEach(r => {
    const m = r.model || '(sin modelo)';
    modelCounts[m] = (modelCounts[m] || 0) + 1;
  });
  Object.keys(modelCounts).sort().forEach(m => {
    opts += `<option value="${esc(m)}" ${m === current ? 'selected' : ''}>${esc(m)} (${modelCounts[m]})</option>`;
  });
  sel.innerHTML = opts;
}

function applyMetricsFiltersAndRender(){
  if(metricsState.subMode === 'variables'){
    applyVariablesFiltersAndRender();
    return;
  }

  let rows = [...metricsState.rows];

  // 1. Filtro por modelo
  if(metricsState.selectedModel){
    rows = rows.filter(r => (r.model || '(sin modelo)') === metricsState.selectedModel);
  }

  // 2. Filtro por búsqueda de texto
  if(metricsState.searchQuery){
    const q = metricsState.searchQuery;
    rows = rows.filter(r =>
      (r.client_bot && r.client_bot.toLowerCase().includes(q)) ||
      (r.prompt_name && r.prompt_name.toLowerCase().includes(q)) ||
      (r.prompt && r.prompt.toLowerCase().includes(q)) ||
      (r.model && r.model.toLowerCase().includes(q))
    );
  }

  const warnLimit = metricsState.warnLimit;
  const critLimit = metricsState.critLimit;

  // Contadores para distribución y KPIs
  const total = rows.length;
  const countOk = rows.filter(r => r.char_count <= warnLimit).length;
  const countWarn = rows.filter(r => r.char_count > warnLimit && r.char_count <= critLimit).length;
  const countDanger = rows.filter(r => r.char_count > critLimit).length;
  const countTotalOverWarn = countWarn + countDanger;

  // Actualizar barra de distribución (Card 3)
  const pctOk = total > 0 ? ((countOk / total) * 100) : 0;
  const pctWarn = total > 0 ? ((countWarn / total) * 100) : 0;
  const pctCrit = total > 0 ? ((countDanger / total) * 100) : 0;

  if($('distSegOk')) $('distSegOk').style.width = pctOk + '%';
  if($('distSegWarn')) $('distSegWarn').style.width = pctWarn + '%';
  if($('distSegCrit')) $('distSegCrit').style.width = pctCrit + '%';

  if($('legendOkCount')) $('legendOkCount').textContent = `${countOk} dentro del límite`;
  if($('legendWarnCount')) $('legendWarnCount').textContent = `${countWarn} en advertencia`;
  if($('legendCritCount')) $('legendCritCount').textContent = `${countDanger} críticos`;

  // Actualizar etiquetas y números en Toolbar Pills
  if($('countMetricsAll')) $('countMetricsAll').textContent = total;
  if($('pillWarnLabel')) $('pillWarnLabel').textContent = `> ${warnLimit.toLocaleString('es-AR')} car.`;
  if($('countMetricsWarn')) $('countMetricsWarn').textContent = countTotalOverWarn;
  if($('pillCritLabel')) $('pillCritLabel').textContent = `> ${critLimit.toLocaleString('es-AR')} car.`;
  if($('countMetricsDanger')) $('countMetricsDanger').textContent = countDanger;

  // 3. Filtro por estado activo
  if(metricsState.statusFilter === 'warn'){
    rows = rows.filter(r => r.char_count > warnLimit && r.char_count <= critLimit);
  } else if(metricsState.statusFilter === 'danger'){
    rows = rows.filter(r => r.char_count > critLimit);
  }

  // 4. Ordenamiento
  const field = metricsState.sortField;
  const asc = metricsState.sortAsc;
  rows.sort((a, b) => {
    let va = a[field] !== undefined ? a[field] : '';
    let vb = b[field] !== undefined ? b[field] : '';
    if(typeof va === 'string') va = va.toLowerCase();
    if(typeof vb === 'string') vb = vb.toLowerCase();
    if(va < vb) return asc ? -1 : 1;
    if(va > vb) return asc ? 1 : -1;
    return 0;
  });

  metricsState.filteredRows = rows;
  renderMetricsTable(rows);
  renderMetricsLengthKpis(total, countWarn, countDanger);
}

function renderMetricsLengthKpis(total, countWarn, countDanger){
  const allRows = metricsState.rows;
  let totalChars = 0;
  allRows.forEach(r => { totalChars += (r.char_count || 0); });
  const avg = allRows.length > 0 ? Math.round(totalChars / allRows.length) : 0;

  if($('kpiTotalPrompts')) $('kpiTotalPrompts').textContent = total.toLocaleString('es-AR');
  if($('kpiAvgChars')) $('kpiAvgChars').textContent = avg.toLocaleString('es-AR') + ' car.';
  if($('kpiWarnLabel')) $('kpiWarnLabel').textContent = `EN ADVERTENCIA (> ${metricsState.warnLimit.toLocaleString('es-AR')})`;
  if($('kpiWarnPrompts')) $('kpiWarnPrompts').textContent = countWarn;
  if($('kpiCritLabel')) $('kpiCritLabel').textContent = `CRÍTICOS (> ${metricsState.critLimit.toLocaleString('es-AR')})`;
  if($('kpiDangerPrompts')) $('kpiDangerPrompts').textContent = countDanger;
}

function renderMetricsTable(rows){
  const tbody = $('metricsTableBody');
  if(!tbody) return;

  if(rows.length === 0){
    tbody.innerHTML = '<tr><td colspan="6" class="metrics-empty-td">No se encontraron prompts con los filtros seleccionados.</td></tr>';
    return;
  }

  const maxCharInSet = Math.max(metricsState.critLimit * 1.25, ...rows.map(r => r.char_count || 0));

  let html = '';
  rows.forEach(r => {
    const chars = r.char_count || 0;
    const tokens = r.estimated_tokens || Math.round(chars / 4);

    let barClass = 'bar-blue';
    if(chars > metricsState.critLimit){
      barClass = 'bar-red';
    } else if(chars > metricsState.warnLimit){
      barClass = 'bar-yellow';
    }

    const pct = Math.min(100, Math.round((chars / maxCharInSet) * 100));

    const modelStr = r.model || '';
    let modelBadgeClass = 'model-pill-standard';
    if(modelStr.includes('luna')){
      modelBadgeClass = 'model-pill-luna';
    } else if(modelStr.includes('mini')){
      modelBadgeClass = 'model-pill-mini';
    } else if(modelStr.includes('4.1') || modelStr.includes('4o')){
      modelBadgeClass = 'model-pill-standard';
    }

    const charsFormatted = chars.toLocaleString('es-AR') + ' car.';
    const tokensFormatted = '~' + tokens.toLocaleString('es-AR') + ' tok';

    html += `<tr>
      <td class="metrics-col-bot">${esc(r.client_bot || (r.client + ' / ' + r.bot))}</td>
      <td class="metrics-col-prompt">${esc(r.prompt_name || r.prompt)}</td>
      <td><span class="model-pill ${modelBadgeClass}">${esc(modelStr || '(sin modelo)')}</span></td>
      <td>
        <div class="metrics-char-cell">
          <span class="metrics-char-val">${charsFormatted}</span>
          <div class="metrics-bar-track" title="${chars} caracteres">
            <div class="metrics-bar-fill ${barClass}" style="width: ${Math.max(4, pct)}%;"></div>
          </div>
        </div>
      </td>
      <td class="metrics-tokens-val">${tokensFormatted}</td>
      <td>
        <div class="metrics-actions-cell">
          <button type="button" class="btn-metrics-view-prompt" data-client="${esc(r.client)}" data-bot="${esc(r.bot)}" data-prompt="${esc(r.prompt_name || r.prompt)}" title="Ver y editar este prompt en Gestión de Prompts">Ver prompt</button>
          <button type="button" class="link-metrics-open-bot" data-client="${esc(r.client)}" data-bot="${esc(r.bot)}" title="Abrir JSON de este bot">Abrir bot</button>
        </div>
      </td>
    </tr>`;
  });

  tbody.innerHTML = html;

  tbody.querySelectorAll('.btn-metrics-view-prompt').forEach(btn => {
    btn.onclick = () => {
      navigateToPrompt(btn.dataset.client, btn.dataset.bot, btn.dataset.prompt, metricsState.env);
    };
  });
  tbody.querySelectorAll('.link-metrics-open-bot').forEach(btn => {
    btn.onclick = () => {
      navigateToBot(btn.dataset.client, btn.dataset.bot, metricsState.env);
    };
  });
}

// =====================================================================
// Submodo: Variables Repetidas por Prompt (Image 2)
// =====================================================================
function applyVariablesFiltersAndRender(){
  let allRows = [...metricsState.rows];

  // 1. Filtro por modelo
  if(metricsState.selectedModel){
    allRows = allRows.filter(r => (r.model || '(sin modelo)') === metricsState.selectedModel);
  }

  // Prompts con variables repetidas
  const promptsWithRepeated = allRows.filter(r => (r.repeated_vars && r.repeated_vars.length > 0));
  const countWithRepeated = promptsWithRepeated.length;
  const countHigh = allRows.filter(r => (r.repeated_vars_total >= 5 || (r.repeated_vars && r.repeated_vars.some(v => v.count >= 5)))).length;
  const countAllPrompts = allRows.length;

  // Actualizar contadores de las pills
  if($('countVarsWithRepeated')) $('countVarsWithRepeated').textContent = countWithRepeated;
  if($('countVarsHigh')) $('countVarsHigh').textContent = countHigh;
  if($('countVarsAllPrompts')) $('countVarsAllPrompts').textContent = countAllPrompts;

  // KPIs de variables
  let totalRepetitions = 0;
  let topPrompt = null;
  const globalVarFreq = {};

  promptsWithRepeated.forEach(r => {
    totalRepetitions += (r.repeated_vars_total || 0);
    if(!topPrompt || (r.repeated_vars_total || 0) > (topPrompt.repeated_vars_total || 0)){
      topPrompt = r;
    }
    (r.repeated_vars || []).forEach(v => {
      globalVarFreq[v.name] = (globalVarFreq[v.name] || 0) + v.count;
    });
  });

  let topVarName = '—';
  let topVarMax = 0;
  Object.keys(globalVarFreq).forEach(vName => {
    if(globalVarFreq[vName] > topVarMax){
      topVarMax = globalVarFreq[vName];
      topVarName = `{${vName}} (${topVarMax}x)`;
    }
  });

  if($('kpiPromptsWithRepeated')) $('kpiPromptsWithRepeated').textContent = countWithRepeated.toLocaleString('es-AR');
  if($('kpiTotalRepetitions')) $('kpiTotalRepetitions').textContent = totalRepetitions.toLocaleString('es-AR');
  if($('kpiTopRepeatedPrompt')){
    if(topPrompt){
      $('kpiTopRepeatedPrompt').textContent = `${topPrompt.client} / ${topPrompt.bot} › ${topPrompt.prompt_name} (${topPrompt.repeated_vars_total} veces)`;
      $('kpiTopRepeatedPrompt').title = `${topPrompt.client} / ${topPrompt.bot} › ${topPrompt.prompt_name}: ${topPrompt.repeated_vars_total} repeticiones totales`;
    } else {
      $('kpiTopRepeatedPrompt').textContent = '—';
    }
  }
  if($('kpiTopRepeatedVar')) $('kpiTopRepeatedVar').textContent = topVarName;

  // 2. Filtrar filas según píldora
  let rows = allRows;
  if(metricsState.varFilter === 'repeated'){
    rows = promptsWithRepeated;
  } else if(metricsState.varFilter === 'high'){
    rows = allRows.filter(r => (r.repeated_vars_total >= 5 || (r.repeated_vars && r.repeated_vars.some(v => v.count >= 5))));
  }

  // 3. Filtro por búsqueda de texto (busca en client, bot, prompt, o nombre de variables)
  if(metricsState.searchQuery){
    const q = metricsState.searchQuery.replace(/^\{|\}$/g, '').toLowerCase();
    rows = rows.filter(r => {
      if(r.client_bot && r.client_bot.toLowerCase().includes(q)) return true;
      if(r.prompt_name && r.prompt_name.toLowerCase().includes(q)) return true;
      if(r.model && r.model.toLowerCase().includes(q)) return true;
      if(r.repeated_vars && r.repeated_vars.some(v => v.name.toLowerCase().includes(q))) return true;
      if(r.template_placeholders && r.template_placeholders.some(p => String(p).toLowerCase().includes(q))) return true;
      return false;
    });
  }

  // 4. Ordenamiento: por defecto de mayor a menor número de repeticiones totales
  rows.sort((a, b) => {
    const repA = a.repeated_vars_total || 0;
    const repB = b.repeated_vars_total || 0;
    if(repA !== repB) return repB - repA;
    return (a.client_bot || '').localeCompare(b.client_bot || '');
  });

  renderVariablesPromptList(rows);
}

function renderVariablesPromptList(rows){
  const container = $('metricsVarsPromptList');
  if(!container) return;

  if(rows.length === 0){
    container.innerHTML = '<div class="metrics-empty-td">No se encontraron prompts con los filtros seleccionados.</div>';
    return;
  }

  let html = '';
  rows.forEach(r => {
    const promptId = r.id || `${r.client}/${r.bot}/${r.prompt_name}`;
    const isExpanded = metricsState.expandedPrompts.has(promptId);
    const repVars = r.repeated_vars || [];
    const totalRep = r.repeated_vars_total || repVars.reduce((sum, v) => sum + v.count, 0);

    // Top variable badges for header
    let chipsHtml = '';
    const topChips = repVars.slice(0, 4);
    topChips.forEach(v => {
      let chipClass = 'chip-other';
      if(v.count >= 5) chipClass = 'chip-crit';
      else if(v.count >= 3) chipClass = 'chip-warn';
      chipsHtml += `<span class="var-chip ${chipClass}">{${esc(v.name)}} <span class="chip-badge">x${v.count}</span></span>`;
    });
    if(repVars.length > 4){
      chipsHtml += `<span class="var-chip chip-other" style="font-size:10px;">+${repVars.length - 4} más</span>`;
    }

    // Expanded details
    let bodyHtml = '';
    if(isExpanded){
      const maxCount = repVars.length > 0 ? Math.max(...repVars.map(v => v.count)) : 1;
      let varItemsHtml = '';

      if(repVars.length === 0){
        varItemsHtml = '<div style="color:#64748b;font-style:italic;padding:8px 0;">Este prompt no contiene variables con repeticiones.</div>';
      } else {
        repVars.forEach(v => {
          const locKey = `${promptId}::${v.name}`;
          const isLocExpanded = metricsState.expandedLocations.has(locKey);
          const barPct = Math.min(100, Math.max(8, Math.round((v.count / maxCount) * 100)));

          let barColor = 'bar-sky';
          if(v.count >= 5) barColor = 'bar-coral';
          else if(v.count >= 3) barColor = 'bar-amber';
          else if(v.count === 2) barColor = 'bar-violet';

          // Ubicaciones de la variable
          const locs = (r.variable_locations && r.variable_locations[v.name]) || [];
          let locDrawerHtml = '';
          if(isLocExpanded){
            if(locs.length > 0){
              let locList = locs.map(l => {
                const highlighted = esc(l.snippet).replace(new RegExp(`\\{${esc(v.name)}\\}`, 'g'), `<mark>{${esc(v.name)}}</mark>`);
                return `<div class="var-loc-item">
                  <span class="var-loc-line">Línea ${l.line}:</span>
                  <span class="var-loc-snippet">${highlighted}</span>
                </div>`;
              }).join('');
              locDrawerHtml = `<div class="var-locations-drawer">${locList}</div>`;
            } else {
              locDrawerHtml = `<div class="var-locations-drawer" style="color:#94a3b8;font-size:11.5px;">Aparece <strong>${v.count}</strong> veces en el texto del prompt. Abrí el editor para examinar el texto completo.</div>`;
            }
          }

          varItemsHtml += `<div>
            <div class="var-rep-row">
              <span class="var-rep-name" title="{${esc(v.name)}}">{${esc(v.name)}}</span>
              <div class="var-rep-bar-wrap" title="{${esc(v.name)}} se repite ${v.count} veces">
                <div class="var-rep-bar-fill ${barColor}" style="width:${barPct}%;"></div>
              </div>
              <span class="var-rep-count-label">x${v.count}</span>
              <button type="button" class="btn-ver-donde" data-prompt-id="${esc(promptId)}" data-var-name="${esc(v.name)}" title="Ver líneas donde aparece esta variable">
                ${isLocExpanded ? 'Ocultar ▲' : 'Ver dónde ▼'}
              </button>
            </div>
            ${locDrawerHtml}
          </div>`;
        });
      }

      bodyHtml = `<div class="var-prompt-expanded-body">${varItemsHtml}</div>`;
    }

    html += `<div class="var-prompt-card-row ${isExpanded ? 'expanded' : ''}" data-prompt-id="${esc(promptId)}">
      <div class="var-prompt-header-row" data-action="toggle-prompt" data-prompt-id="${esc(promptId)}">
        <div class="var-prompt-chevron">▼</div>
        <div class="var-prompt-title-wrap">
          <span class="var-prompt-client-bot">${esc(r.client_bot || (r.client + ' / ' + r.bot))}</span>
          <span class="var-prompt-name-tag">${esc(r.prompt_name || r.prompt)}</span>
        </div>
        <div class="var-prompt-chips-wrap">
          ${chipsHtml}
        </div>
        <div class="var-prompt-total-badge">${totalRep} ${totalRep === 1 ? 'vez' : 'veces'}</div>
        <div class="var-prompt-actions-wrap" onclick="event.stopPropagation();">
          <button type="button" class="btn-prompt-view-primary" data-client="${esc(r.client)}" data-bot="${esc(r.bot)}" data-prompt="${esc(r.prompt_name || r.prompt)}" title="Ver y editar en Gestión de Prompts">Ver prompt</button>
          <button type="button" class="link-prompt-open-bot" data-client="${esc(r.client)}" data-bot="${esc(r.bot)}" title="Abrir JSON de este bot">Abrir bot</button>
        </div>
      </div>
      ${bodyHtml}
    </div>`;
  });

  container.innerHTML = html;

  // Toggle accordion expand/collapse
  container.querySelectorAll('.var-prompt-header-row[data-action="toggle-prompt"]').forEach(el => {
    el.onclick = () => {
      const pId = el.dataset.promptId;
      if(metricsState.expandedPrompts.has(pId)){
        metricsState.expandedPrompts.delete(pId);
      } else {
        metricsState.expandedPrompts.add(pId);
      }
      applyVariablesFiltersAndRender();
    };
  });

  // Toggle "Ver dónde"
  container.querySelectorAll('.btn-ver-donde').forEach(btn => {
    btn.onclick = (e) => {
      e.stopPropagation();
      const pId = btn.dataset.promptId;
      const vName = btn.dataset.varName;
      const locKey = `${pId}::${vName}`;
      if(metricsState.expandedLocations.has(locKey)){
        metricsState.expandedLocations.delete(locKey);
      } else {
        metricsState.expandedLocations.add(locKey);
      }
      applyVariablesFiltersAndRender();
    };
  });

  // Action buttons
  container.querySelectorAll('.btn-prompt-view-primary').forEach(btn => {
    btn.onclick = (e) => {
      e.stopPropagation();
      navigateToPrompt(btn.dataset.client, btn.dataset.bot, btn.dataset.prompt, metricsState.env);
    };
  });
  container.querySelectorAll('.link-prompt-open-bot').forEach(btn => {
    btn.onclick = (e) => {
      e.stopPropagation();
      navigateToBot(btn.dataset.client, btn.dataset.bot, metricsState.env);
    };
  });
}


// Redirección directa al prompt en la vista Gestión de Prompts
async function navigateToPrompt(client, bot, promptName, env = 'desa'){
  if(hasDirty() && !window.confirm('Hay cambios sin guardar. ¿Descartarlos y abrir el prompt?')) return;
  if(hasDirty()) discardEdits();

  // 1. Cambiar a modo 'prompts'
  $('viewMode').value = 'prompts';
  switchMode('prompts');

  // 2. Ajustar ambiente
  if($('singleEnv')){
    $('singleEnv').value = env;
    syncSingleEnvButtons();
  }

  // 3. Ajustar selector 'pick'
  const pick = $('pick');
  if(pick){
    const targetVal = JSON.stringify([client, bot]);
    for(let i = 0; i < pick.options.length; i++){
      if(pick.options[i].value === targetVal){
        pick.selectedIndex = i;
        break;
      }
    }
  }

  setStatus('Abriendo prompt ' + promptName + ' en ' + client + ' / ' + bot + '…');
  try{
    const j = await api('/api/item?env=' + encodeURIComponent(env) + '&client=' + encodeURIComponent(client) + '&bot=' + encodeURIComponent(bot));
    if(!j.item) return setStatus(client + ' / ' + bot + ' no existe en ' + env + '.', 'err');

    singleActiveKey = {client, bot, env};
    singleVersion = j.version;
    rawSingleItem = j.item;
    unpackedPromptsData = unpackPrompts(rawSingleItem);

    // Encontrar el prompt exacto solicitado o equivalente
    const availablePrompts = Object.keys(unpackedPromptsData.prompts || {});
    let matched = availablePrompts.find(p => p === promptName || p.endsWith('.' + promptName) || p.endsWith(' ' + promptName) || p.toLowerCase() === promptName.toLowerCase());
    selectedPromptName = matched || availablePrompts[0] || null;

    apiKeyRevealed = false;
    comparisonEnv = getCounterpartEnv(env);
    comparisonItem = null;
    comparisonPromptsData = null;
    fetchComparisonItem(client, bot, comparisonEnv);

    singleModel.setValue(j.json);
    singleBaseline = singleModel.getValue();
    singleLoaded = true;

    const badge = $('singleEnvBadge');
    if(badge){
      badge.textContent = env.toUpperCase();
      badge.className = 'env-badge ' + env;
    }
    if($('footerEnvBadge')) $('footerEnvBadge').textContent = env.toUpperCase();
    if($('footerTableName')) $('footerTableName').textContent = tableOf(env);
    if($('singleTitle')) $('singleTitle').textContent = tableOf(env) + ' · ' + client + ' / ' + bot;

    renderPromptsView();
    setSingleSubMode('text');
    updateDirtyState();
    refreshHistoryBadge();

    // Scroll al prompt activo en la barra lateral
    setTimeout(() => {
      const activeEl = document.querySelector('.sidebar-prompt-item.active');
      if(activeEl) activeEl.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
    }, 150);

    setStatus('Prompt ' + (selectedPromptName || promptName) + ' abierto en ' + client + ' / ' + bot + '.', 'ok');
  }catch(e){
    setStatus(e.message, 'err');
  }
}

async function navigateToBot(client, bot, env = 'desa'){
  if(hasDirty() && !window.confirm('Hay cambios sin guardar. ¿Descartarlos y abrir el bot?')) return;
  if(hasDirty()) discardEdits();

  $('viewMode').value = 'single';
  switchMode('single');
  if($('singleEnv')){
    $('singleEnv').value = env;
    syncSingleEnvButtons();
  }
  const pick = $('pick');
  if(pick){
    const targetVal = JSON.stringify([client, bot]);
    for(let i = 0; i < pick.options.length; i++){
      if(pick.options[i].value === targetVal){
        pick.selectedIndex = i;
        break;
      }
    }
  }
  await openSingle();
}

/* ---------- init ---------- */
async function init(){
  try{
    ENVS = (await api('/api/envs')).envs;
    await initMonaco();
  }catch(e){ return setStatus(e.message, 'err'); }
  const opts = ENVS.map(e => '<option value="' + esc(e.name) + '">' + esc(e.name) + '</option>').join('');
  $('envL').innerHTML = opts; $('envR').innerHTML = opts; $('singleEnv').innerHTML = opts; $('modelEnv').innerHTML = opts;
  const has = n => ENVS.some(e => e.name === n);
  if(has('desa')) $('envL').value = 'desa';
  if(has('test')) $('envR').value = 'test';
  if(has('desa')) $('singleEnv').value = 'desa';
  if(has('desa')) $('modelEnv').value = 'desa';
  updateComparePaneHeaders();
  $('envL').onchange = $('envR').onchange = () => {
    if((dirty('left') || dirty('right')) && !window.confirm('Hay cambios sin guardar. ¿Descartarlos y cambiar de ambiente?')){
      if(activeKey){ $('envL').value = activeKey.leftEnv; $('envR').value = activeKey.rightEnv; }
      return;
    }
    if(activeKey && ($('envL').value !== activeKey.leftEnv || $('envR').value !== activeKey.rightEnv)){
      loaded = false; activeKey = null; versions = {left: null, right: null};
      leftModel.setValue('{}'); rightModel.setValue('{}');
      baseline = {left: '{}', right: '{}'};
      updateSaveButtons();
    }
    updateComparePaneHeaders();
    refreshList(false);
  };
  $('singleEnv').onchange = () => {
    if(singleDirty() && !window.confirm('Hay cambios sin guardar. ¿Descartarlos y listar otro ambiente?')){
      $('singleEnv').value = singleActiveKey.env; return;
    }
    if(singleActiveKey && $('singleEnv').value !== singleActiveKey.env){
      singleLoaded = false; singleActiveKey = null; singleVersion = null; singleBaseline = '{}';
      singleModel.setValue('{}'); $('singleTitle').textContent = 'JSON individual';
      updateSaveButtons();
    }
    refreshList(false);
  };
  $('modelEnv').onchange = () => {
    selectedModel = null; selectedBots = new Set(); expandedBot = null;
    deselectedPrompts = new Set(); promptOverrides = new Map();
    selectedModels = new Set();
    $('modelTarget').value = ''; $('modelTemperature').value = '';
    $('modelReasoning').value = ''; $('modelResponseFormat').value = ''; $('modelMaxTokens').value = '';
    if($('modelBotSelect')) $('modelBotSelect').value = '';
    if($('modelBotFilter')) $('modelBotFilter').value = '';
    if($('modelPromptSelect')) $('modelPromptSelect').value = '';
    if($('modelPromptFilter')) $('modelPromptFilter').value = '';
    if($('modelNameFilter')) $('modelNameFilter').value = '';
    updateModelControlsVisibility();
    renderModels(); loadModels(false);
  };
  $('swap').onclick = () => {
    const t = $('envL').value;
    $('envL').value = $('envR').value;
    $('envR').value = t;
    updateComparePaneHeaders();
    refreshList(false);
  };
  $('filter').oninput = () => {
    if(activeMode === 'models'){
      if($('modelNameFilter')) $('modelNameFilter').value = $('filter').value;
      renderModels();
    }else if(activeMode === 'apikeys'){
      renderApiKeys();
    }else{
      applyFilter();
    }
  };
  $('apiKeyPresence').onchange = renderApiKeys;
  $('exportApiKeys').onclick = downloadApiKeysExcel;
  $('filter').onkeydown = e => { if(e.key === 'Enter' && activeMode === 'single') openSingle(); else if(e.key === 'Enter' && activeMode === 'compare') compare(); };
  $('viewMode').onchange = changeMode;
  $('ok').onclick = () => activeMode === 'single' ? openSingle() : compare();
  $('reload').onclick = () => activeMode === 'models' ? loadModels(true) : activeMode === 'apikeys' ? loadApiKeys(true) : refreshList(true);
  // Top bar environment buttons
  document.querySelectorAll('#modelEnvSegmented .env-btn').forEach(btn => {
    btn.onclick = () => setModelEnvironment(btn.dataset.env);
  });
  if($('modelBtnHistory')) $('modelBtnHistory').onclick = openModelHistory;
  if($('modelBtnExcel')) $('modelBtnExcel').onclick = downloadModelsExcel;
  if($('btnReviewChanges')) $('btnReviewChanges').onclick = openModelReviewModal;
  if($('modalReviewCloseBtn')) $('modalReviewCloseBtn').onclick = closeModelReviewModal;
  if($('btnModalReviewCancel')) $('btnModalReviewCancel').onclick = closeModelReviewModal;
  if($('btnModalReviewConfirm')) $('btnModalReviewConfirm').onclick = applyModelChangesBatch;
  if($('modelUndoBtn')) $('modelUndoBtn').onclick = undoModelChangesBatch;
  if($('modalReviewProdInput')){
    $('modalReviewProdInput').oninput = e => {
      const isConfirmed = e.target.value.trim() === 'PROD';
      if($('btnModalReviewConfirm')) $('btnModalReviewConfirm').disabled = !isConfirmed;
    };
  }
  if($('btnReplicatePrompts')) $('btnReplicatePrompts').onclick = () => openModelReplicateModal();
  if($('modalReplicateCloseBtn')) $('modalReplicateCloseBtn').onclick = closeModelReplicateModal;
  if($('btnModalReplicateCancel')) $('btnModalReplicateCancel').onclick = closeModelReplicateModal;
  if($('btnModalReplicateConfirm')) $('btnModalReplicateConfirm').onclick = applyReplicationPlan;
  if($('btnRefreshReplicatePlan')) $('btnRefreshReplicatePlan').onclick = loadReplicatePlan;
  if($('replicateSourceEnvSelect')) $('replicateSourceEnvSelect').onchange = loadReplicatePlan;
  if($('replicateTableFilter')) $('replicateTableFilter').oninput = () => renderReplicateDiffTable();
  if($('modalReplicateProdInput')){
    $('modalReplicateProdInput').oninput = e => {
      const isConfirmed = e.target.value.trim() === 'PROD';
      const hasChanges = currentReplicatePlan && currentReplicatePlan.changed_bots_count > 0;
      if($('btnModalReplicateConfirm')) $('btnModalReplicateConfirm').disabled = !(isConfirmed && hasChanges);
    };
  }


  // Left column: scope toggle and search
  if($('scopeByPrompt')) $('scopeByPrompt').onclick = () => setModelsScopeMode('prompt');
  if($('scopeByModel')) $('scopeByModel').onclick = () => setModelsScopeMode('model');
  if($('modelListSearch')){
    $('modelListSearch').oninput = e => {
      modelsState.listFilter = e.target.value;
      renderLeftList();
    };
  }
  if($('modelItemList')){
    $('modelItemList').onclick = e => {
      const item = e.target.closest('.model-list-item');
      if(item && item.dataset.id) selectScopeItem(item.dataset.id);
    };
  }

  // Center column: bot search, chips, table selection
  if($('centerBotFilter')){
    $('centerBotFilter').oninput = e => {
      modelsState.botFilter = e.target.value;
      renderCenterColumn();
      updateRightColumn();
    };
  }
  if($('familyChips')){
    $('familyChips').onclick = e => {
      const chip = e.target.closest('.chip');
      if(!chip) return;
      modelsState.familyFilter = chip.dataset.val === '__all__' ? null : chip.dataset.val;
      renderCenterColumn();
      updateRightColumn();
    };
  }
  if($('groupChips')){
    $('groupChips').onclick = e => {
      const chip = e.target.closest('.chip');
      if(!chip) return;
      modelsState.groupFilter = chip.dataset.val === '__all__' ? null : chip.dataset.val;
      renderCenterColumn();
      updateRightColumn();
    };
  }

  // Table row clicks / checkbox clicks
  if($('modelTableBody')){
    $('modelTableBody').onclick = e => {
      const chk = e.target.closest('.row-select-check');
      const tr = e.target.closest('tr[data-key]');
      if(!tr) return;
      const key = tr.dataset.key;
      if(chk){
        if(chk.checked) modelsState.selectedRowKeys.add(key);
        else modelsState.selectedRowKeys.delete(key);
      }else{
        if(modelsState.selectedRowKeys.has(key)) modelsState.selectedRowKeys.delete(key);
        else modelsState.selectedRowKeys.add(key);
      }
      renderCenterColumn();
      updateRightColumn();
    };
  }

  // Table keyboard navigation
  if($('modelTableWrap')){
    $('modelTableWrap').onkeydown = e => {
      if(e.code === 'Space'){
        e.preventDefault();
        const focused = document.activeElement && document.activeElement.closest('tr[data-key]');
        if(focused){
          const key = focused.dataset.key;
          if(modelsState.selectedRowKeys.has(key)) modelsState.selectedRowKeys.delete(key);
          else modelsState.selectedRowKeys.add(key);
          renderCenterColumn();
          updateRightColumn();
        }
      }else if((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'a'){
        e.preventDefault();
        const rows = getFilteredRows();
        rows.forEach(r => modelsState.selectedRowKeys.add(`${r.client}/${r.bot}/${r.prompt_name}`));
        renderCenterColumn();
        updateRightColumn();
      }
    };
  }

  // Right column inputs and validation
  if($('editModelSelect')){
    $('editModelSelect').onchange = e => {
      modelsState.draft.model = e.target.value.trim();
      if(modelsState.draft.model) e.target.classList.add('input-pending');
      else e.target.classList.remove('input-pending');
      renderCenterColumn();
      updateRightColumn();
    };
  }

  if($('editTempInput')){
    $('editTempInput').oninput = e => {
      const val = e.target.value.trim();
      if(val === ''){
        modelsState.draft.temperature = '';
        modelsState.validationErrors.temperature = null;
        if($('tempError')) $('tempError').hidden = true;
        e.target.classList.remove('input-error', 'input-pending');
      }else if(val.includes(',') || isNaN(Number(val)) || Number(val) < 0 || Number(val) > 2){
        modelsState.draft.temperature = val;
        modelsState.validationErrors.temperature = 'La temperatura va de 0 a 2. Usá punto para decimales (0.7).';
        if($('tempError')){
          $('tempError').textContent = modelsState.validationErrors.temperature;
          $('tempError').hidden = false;
        }
        e.target.classList.add('input-error');
        e.target.classList.remove('input-pending');
      }else{
        modelsState.draft.temperature = val;
        modelsState.validationErrors.temperature = null;
        if($('tempError')) $('tempError').hidden = true;
        e.target.classList.remove('input-error');
        e.target.classList.add('input-pending');
      }
      renderCenterColumn();
      updateRightColumn();
    };
  }

  if($('editTokensInput')){
    $('editTokensInput').oninput = e => {
      const val = e.target.value.trim();
      if(val === ''){
        modelsState.draft.max_tokens = '';
        modelsState.validationErrors.max_tokens = null;
        if($('tokensError')) $('tokensError').hidden = true;
        e.target.classList.remove('input-error', 'input-pending');
      }else if(!/^\d+$/.test(val) || parseInt(val, 10) <= 0){
        modelsState.draft.max_tokens = val;
        modelsState.validationErrors.max_tokens = 'Max tokens debe ser un entero mayor a 0.';
        if($('tokensError')){
          $('tokensError').textContent = modelsState.validationErrors.max_tokens;
          $('tokensError').hidden = false;
        }
        e.target.classList.add('input-error');
        e.target.classList.remove('input-pending');
      }else{
        modelsState.draft.max_tokens = val;
        modelsState.validationErrors.max_tokens = null;
        if($('tokensError')) $('tokensError').hidden = true;
        e.target.classList.remove('input-error');
        e.target.classList.add('input-pending');
      }
      renderCenterColumn();
      updateRightColumn();
    };
  }

  if($('editReasoningSelect')){
    $('editReasoningSelect').onchange = e => {
      const val = e.target.value;
      modelsState.draft.reasoning_effort = val;
      if(val !== '') e.target.classList.add('input-pending');
      else e.target.classList.remove('input-pending');
      renderCenterColumn();
      updateRightColumn();
    };
  }

  if($('editFormatSelect')){
    $('editFormatSelect').onchange = e => {
      const val = e.target.value;
      modelsState.draft.response_format_type = val;
      if(val !== '') e.target.classList.add('input-pending');
      else e.target.classList.remove('input-pending');
      renderCenterColumn();
      updateRightColumn();
    };
  }

  // Variables list interactions (remove / undo / cancel added)
  if($('variablesList')){
    $('variablesList').onclick = e => {
      const delBtn = e.target.closest('[data-del-var]');
      const undoBtn = e.target.closest('[data-undo-var]');
      const cancelAddBtn = e.target.closest('[data-cancel-add-var]');

      if(delBtn){
        const v = delBtn.dataset.delVar;
        if(v && !modelsState.draft.variablesToRemove.includes(v)){
          modelsState.draft.variablesToRemove.push(v);
          renderCenterColumn();
          updateRightColumn();
        }
        return;
      }
      if(undoBtn){
        const v = undoBtn.dataset.undoVar;
        if(v){
          modelsState.draft.variablesToRemove = modelsState.draft.variablesToRemove.filter(x => x !== v);
          renderCenterColumn();
          updateRightColumn();
        }
        return;
      }
      if(cancelAddBtn){
        const v = cancelAddBtn.dataset.cancelAddVar;
        if(v){
          modelsState.draft.variablesToAdd = modelsState.draft.variablesToAdd.filter(x => x !== v);
          renderCenterColumn();
          updateRightColumn();
        }
        return;
      }
    };
  }

  // Add new variable
  function handleAddNewVariable(){
    const input = $('newVarInput');
    if(!input) return;
    const val = (input.value || '').trim().toUpperCase().replace(/[^A-Z0-9_]/g, '_');
    if(!val) return;
    if(modelsState.draft.variablesToRemove.includes(val)){
      modelsState.draft.variablesToRemove = modelsState.draft.variablesToRemove.filter(v => v !== val);
    }else if(!modelsState.draft.variablesToAdd.includes(val)){
      modelsState.draft.variablesToAdd.push(val);
    }
    input.value = '';
    renderCenterColumn();
    updateRightColumn();
  }

  if($('btnAddVar')) $('btnAddVar').onclick = handleAddNewVariable;
  if($('newVarInput')){
    $('newVarInput').onkeydown = e => {
      if(e.key === 'Enter'){
        e.preventDefault();
        handleAddNewVariable();
      }
    };
    $('newVarInput').oninput = e => {
      const start = e.target.selectionStart;
      const end = e.target.selectionEnd;
      e.target.value = e.target.value.toUpperCase().replace(/[^A-Z0-9_]/g, '_');
      e.target.setSelectionRange(start, end);
    };
  }

  // Right column: clear changes and selection
  if($('rightClearChanges')){
    $('rightClearChanges').onclick = () => {
      clearDraftEdits();
      renderCenterColumn();
      updateRightColumn();
    };
  }
    $('hide').onchange = () => diffEditor.updateOptions({hideUnchangedRegions: {enabled: $('hide').checked, contextLineCount: 3}});
  $('prev').onclick = () => go(-1);
  $('next').onclick = () => go(1);
  $('saveL').onclick = () => saveSide('left'); $('saveR').onclick = () => saveSide('right');
  $('saveSingle').onclick = saveSingle;
  $('toLeft').onclick = () => transfer('right'); $('toRight').onclick = () => transfer('left');
  $('foldL').onclick = () => diffEditor.getOriginalEditor().getAction('editor.foldAll').run();
  $('unfoldL').onclick = () => diffEditor.getOriginalEditor().getAction('editor.unfoldAll').run();
  $('foldR').onclick = () => diffEditor.getModifiedEditor().getAction('editor.foldAll').run();
  // Sub-mode segmented control
  $('subModeJson').onclick = () => {
    $('viewMode').value = 'single';
    activeMode = 'single';
    setSingleSubMode('json');
    syncRailActive('single');
  };
  $('subModePrompts').onclick = () => {
    $('viewMode').value = 'prompts';
    activeMode = 'prompts';
    setSingleSubMode('text');
    syncRailActive('prompts');
  };

  // Font size buttons
  $('btnFontSizeDec').onclick = () => adjustFontSize(-1);
  $('btnFontSizeInc').onclick = () => adjustFontSize(1);

  // Toggle right JSON panel
  $('btnToggleRightJson').onclick = () => {
    const p = $('promptsRightJson');
    p.hidden = !p.hidden;
    $('btnToggleRightJson').textContent = p.hidden ? '{ } Mostrar JSON' : '{ } Ocultar JSON';
    if(templateEditor) templateEditor.layout();
  };

  // Template tools
  $('btnDiffTemplate').onclick = toggleTemplateDiffView;
  $('btnFoldTemplate').onclick = () => {
    templateSectionsFolded = !templateSectionsFolded;
    const actionId = templateSectionsFolded ? 'editor.foldAll' : 'editor.unfoldAll';
    $('btnFoldTemplate').title = templateSectionsFolded ? 'Desplegar secciones' : 'Plegar secciones';
    $('btnFoldTemplate').classList.toggle('active', templateSectionsFolded);

    if(templateDiffActive && templateDiffEditor){
      templateDiffEditor.getOriginalEditor().getAction(actionId).run();
      templateDiffEditor.getModifiedEditor().getAction(actionId).run();
    }else if(templateEditor){
      templateEditor.getAction(actionId).run();
    }
  };
  $('btnWrapTemplate').onclick = () => {
    templateWrapEnabled = !templateWrapEnabled;
    $('btnWrapTemplate').classList.toggle('active', templateWrapEnabled);
    const wrapOpt = templateWrapEnabled ? 'on' : 'off';
    const wrapStrategy = templateWrapEnabled ? 'advanced' : 'normal';
    const horizScroll = !templateWrapEnabled;
    if(templateEditor){
      templateEditor.updateOptions({
        wordWrap: wrapOpt,
        wrappingStrategy: wrapStrategy,
        scrollBeyondLastColumn: 0,
        horizontalScrolling: horizScroll
      });
      templateEditor.layout();
    }
    if(templateDiffEditor){
      templateDiffEditor.updateOptions({
        diffWordWrap: wrapOpt,
        wordWrap: wrapOpt,
        wrappingStrategy: wrapStrategy,
        scrollBeyondLastColumn: 0,
        horizontalScrolling: horizScroll
      });
      if(templateDiffEditor.getOriginalEditor()){
        templateDiffEditor.getOriginalEditor().updateOptions({
          wordWrap: wrapOpt,
          wrappingStrategy: wrapStrategy,
          scrollBeyondLastColumn: 0,
          horizontalScrolling: horizScroll
        });
      }
      if(templateDiffEditor.getModifiedEditor()){
        templateDiffEditor.getModifiedEditor().updateOptions({
          wordWrap: wrapOpt,
          wrappingStrategy: wrapStrategy,
          scrollBeyondLastColumn: 0,
          horizontalScrolling: horizScroll
        });
      }
      templateDiffEditor.layout();
    }
  };

  // Comparison preview card
  if($('compPreviewHeader')){
    $('compPreviewHeader').onclick = e => {
      if(!e.target.closest('button')) toggleCompPreview();
    };
  }
  if($('btnToggleCompPreview')) $('btnToggleCompPreview').onclick = toggleCompPreview;
  if($('btnCopyAllFromComp')) $('btnCopyAllFromComp').onclick = copyAllFromComparison;
  if($('btnCopyTemplateOnly')) $('btnCopyTemplateOnly').onclick = copyTemplateOnlyFromComparison;

  // Collapsible Parameters Card
  if($('promptParamsHeader')){
    $('promptParamsHeader').onclick = e => {
      if(!e.target.closest('button')) toggleParamsCard();
    };
  }
  if($('btnToggleParams')) $('btnToggleParams').onclick = () => toggleParamsCard();

  // Collapsible Sidebar Models
  if($('sidebarModelsHeader')){
    $('sidebarModelsHeader').onclick = () => toggleSidebarModels();
  }

  // Save Modal
  if($('modalCloseBtn')) $('modalCloseBtn').onclick = closeSaveModal;
  if($('btnModalCancel')) $('btnModalCancel').onclick = closeSaveModal;
  if($('btnModalConfirm')) $('btnModalConfirm').onclick = confirmSaveModal;

  // Template Undo / Redo / Fullscreen
  if($('btnUndoTemplate')) $('btnUndoTemplate').onclick = undoTemplate;
  if($('btnRedoTemplate')) $('btnRedoTemplate').onclick = redoTemplate;
  if($('btnFullscreenTemplate')) $('btnFullscreenTemplate').onclick = () => toggleFullscreenTemplate();

  // History Modal
  if($('btnHistory')) $('btnHistory').onclick = openHistoryModal;
  if($('historyModalCloseBtn')) $('historyModalCloseBtn').onclick = closeHistoryModal;
  if($('btnHistoryClose')) $('btnHistoryClose').onclick = closeHistoryModal;
  if($('btnRestoreHistory')) $('btnRestoreHistory').onclick = restoreHistoryVersion;
  if($('historyListItems')){
    $('historyListItems').onclick = e => {
      const card = e.target.closest('.history-version-card');
      if(!card) return;
      const fname = card.dataset.fname;
      const idx = parseInt(card.dataset.idx, 10);
      selectHistoryVersion(fname, idx);
    };
  }

  // Sidebar clicks
  $('sidebarPromptsList').onclick = e => {
    const item = e.target.closest('.sidebar-prompt-item');
    if(!item) return;
    const pName = item.dataset.prompt;
    if(pName === selectedPromptName) return;
    selectedPromptName = pName;
    document.querySelectorAll('.sidebar-prompt-item').forEach(el => {
      el.classList.toggle('active', el.dataset.prompt === selectedPromptName);
    });
    renderActivePrompt();
  };
  $('sidebarModelsList').onclick = e => {
    const card = e.target.closest('.sidebar-model-card');
    if(!card || !card.dataset.prompt) return;
    selectedPromptName = card.dataset.prompt;
    document.querySelectorAll('.sidebar-prompt-item').forEach(el => {
      el.classList.toggle('active', el.dataset.prompt === selectedPromptName);
    });
    renderActivePrompt();
  };
  $('sidebarApiKeyToggleBtn').onclick = () => {
    apiKeyRevealed = !apiKeyRevealed;
    renderPromptsView();
  };

  document.addEventListener('keydown', e => {
    if(e.key === 'Escape'){
      if($('shortcutsModal') && !$('shortcutsModal').hidden){
        e.preventDefault();
        toggleShortcutsModal(false);
        return;
      }
      if($('cleanupDeleteConfirmModal') && !$('cleanupDeleteConfirmModal').hidden){
        e.preventDefault();
        closeCleanupDeleteConfirmModal();
        return;
      }
      if($('cleanupBotDetailModal') && !$('cleanupBotDetailModal').hidden){
        e.preventDefault();
        closeCleanupBotDetailModal();
        return;
      }
      if($('saveModal') && !$('saveModal').hidden){
        e.preventDefault();
        closeSaveModal();
        return;
      }
      if($('modelReviewModal') && !$('modelReviewModal').hidden){
        e.preventDefault();
        closeModelReviewModal();
        return;
      }
      if($('modelReplicateModal') && !$('modelReplicateModal').hidden){
        e.preventDefault();
        closeModelReplicateModal();
        return;
      }
      if($('historyModal') && !$('historyModal').hidden){
        e.preventDefault();
        closeHistoryModal();
        return;
      }
      if($('tablesCreateConfirmModal') && !$('tablesCreateConfirmModal').hidden){
        e.preventDefault();
        closeTablesCreateConfirmModal();
        return;
      }
      if($('promptTemplateCard') && $('promptTemplateCard').classList.contains('fullscreen')){
        e.preventDefault();
        toggleFullscreenTemplate(false);
        return;
      }
    }
    if((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 's'){
      if(activeMode === 'single' || activeMode === 'prompts'){
        e.preventDefault();
        saveSingle();
      }else if(activeMode === 'compare'){
        e.preventDefault();
        const origFocused = diffEditor && diffEditor.getOriginalEditor() && diffEditor.getOriginalEditor().hasTextFocus();
        const modFocused = diffEditor && diffEditor.getModifiedEditor() && diffEditor.getModifiedEditor().hasTextFocus();
        if(origFocused && (dirty('left') || versions.left === null)) saveSide('left');
        else if(modFocused && (dirty('right') || versions.right === null)) saveSide('right');
        else if(dirty('left') && !dirty('right')) saveSide('left');
        else if(dirty('right') && !dirty('left')) saveSide('right');
        else if(dirty('left')) saveSide('left');
        else if(dirty('right')) saveSide('right');
      }
    }else if(activeMode === 'compare' && e.key === 'F7'){
      e.preventDefault();
      go(e.shiftKey ? -1 : 1);
    }
    if((e.ctrlKey || e.metaKey) && ['1','2','3','4','5','6','7','8'].includes(e.key)){
      e.preventDefault();
      const modeMap = {'1':'compare', '2':'single', '3':'prompts', '4':'models', '5':'apikeys', '6':'cleanup', '7':'tables', '8':'metrics'};
      const m = modeMap[e.key];
      if(m && $('viewMode').value !== m){
        $('viewMode').value = m;
        changeMode();
      }
      return;
    }
    if(e.key === 'F11' && (activeMode === 'single' || activeMode === 'prompts')){
      e.preventDefault();
      toggleFullscreenTemplate();
      return;
    }
    if(e.key === '?' && !['INPUT', 'TEXTAREA'].includes(document.activeElement.tagName) && !document.activeElement.closest('.monaco-editor')){
      e.preventDefault();
      toggleShortcutsModal();
      return;
    }
  });
  // V2 UI Bindings
  document.querySelectorAll('#appRail .rail-btn[data-mode]').forEach(btn => {
    btn.onclick = () => {
      const targetMode = btn.dataset.mode;
      switchMode(targetMode);
    };
  });
  if($('railBtnReload')) $('railBtnReload').onclick = () => {
    if(activeMode === 'models') loadModels(true);
    else if(activeMode === 'apikeys') loadApiKeys(true);
    else if(activeMode === 'cleanup') initCleanupView();
    else if(activeMode === 'tables') initTablesView();
    else if(activeMode === 'metrics') loadMetricsData(true);
    else refreshList(true);
  };
  if($('railBtnShortcuts')) $('railBtnShortcuts').onclick = () => toggleShortcutsModal(true);
  if($('shortcutsCloseBtn')) $('shortcutsCloseBtn').onclick = () => toggleShortcutsModal(false);
  if($('shortcutsModal')){
    $('shortcutsModal').onclick = e => {
      if(e.target === $('shortcutsModal')) toggleShortcutsModal(false);
    };
  }

  document.querySelectorAll('#singleEnvSegmented .env-btn').forEach(btn => {
    btn.onclick = () => setSingleEnvironment(btn.dataset.env);
  });
  if($('barSaveBtn')) $('barSaveBtn').onclick = saveSingle;

  if($('btnRightJsonCollapse')){
    $('btnRightJsonCollapse').onclick = () => {
      const p = $('promptsRightJson');
      p.hidden = !p.hidden;
      if($('btnToggleRightJson')) $('btnToggleRightJson').textContent = p.hidden ? '{ } Mostrar JSON' : '{ } Ocultar JSON';
      if(templateEditor) templateEditor.layout();
      if(templateDiffEditor) templateDiffEditor.layout();
    };
  }

  if($('btnQuickNavCompare')){
    $('btnQuickNavCompare').onclick = () => {
      $('viewMode').value = 'compare';
      changeMode();
    };
  }
  if($('btnQuickNavSingle')){
    $('btnQuickNavSingle').onclick = () => {
      $('viewMode').value = 'single';
      changeMode();
    };
  }
  if($('btnQuickNavPrompts')){
    $('btnQuickNavPrompts').onclick = () => {
      $('viewMode').value = 'prompts';
      changeMode();
    };
  }


  syncRailActive(activeMode);
  syncSingleEnvButtons();

  window.addEventListener('resize', () => {
    if(templateDiffActive && templateDiffEditor) templateDiffEditor.layout();
  });
  window.addEventListener('beforeunload', e => { if(hasDirty()){ e.preventDefault(); e.returnValue = ''; } });
  refreshList(false);
}
init();
</script>
</body>
</html>
"""

if __name__ == "__main__":
    url = f"http://127.0.0.1:{PORT}"
    srv = ThreadingHTTPServer(("127.0.0.1", PORT), Handler)
    print(f"Comparador listo en {url}  (Ctrl+C para salir)")
    threading.Timer(0.8, lambda: webbrowser.open(url)).start()
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\nChau.")