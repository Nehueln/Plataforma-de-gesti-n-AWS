#!/usr/bin/env python3
"""
models_manager.py
Lógica de negocio (sin UI) para la vista 'Modelos'.

Etapa 1:
- Scan proyectado y paginado de un ambiente de DynamoDB.
- Lista plana de filas: una por cada pareja (bot + prompt).
- Índices en memoria: prompt -> filas y modelo -> filas.
- Estadísticas de tipos DynamoDB por campo (para elegir el tipo cuando un campo no existe).

Dónde se buscan prompts (verificado con datos reales):
- AGENT_CONFIG.PROMPTS.<X>                      -> etiqueta "X"
- AGENT_CONFIG.<GRUPO>.PROMPTS.<X>              -> etiqueta "GRUPO › X"
  (hoy: SENTIMENT_ANALYSIS, RESUME_CONVERSATIONS; cualquier grupo nuevo se detecta solo)
- <CLAVE_RAIZ> con OPENAI_MODEL (hoy: PROMPT_ORCHESTRATOR) -> etiqueta "(raíz) CLAVE"

Uso por consola (solo lectura, no imprime secretos):
  python models_manager.py desa
"""

import hashlib
import json
import re
import sys
import time
from collections import Counter

# Claves de campos de prompt en DynamoDB (nombres verificados en los datos reales)
FIELD_MODEL = "OPENAI_MODEL"
FIELD_TEMP = "TEMPERATURE"
FIELD_REASONING = "REASONING_EFFORT"
FIELD_FORMAT = "RESPONSE_FORMAT_TYPE"
FIELD_TOKENS = "MAX_COMPLETION_TOKENS"
FIELD_VARS = "INPUT_VARIABLE"
FIELD_TEMPLATE = "TEMPLATE"

# Campos que la vista puede modificar. Todo lo demás se conserva intacto.
EDITABLE_FIELDS = (FIELD_MODEL, FIELD_TEMP, FIELD_REASONING, FIELD_FORMAT, FIELD_TOKENS, FIELD_VARS)

# Nombre de cada campo escalar en la fila plana que viaja al frontend
ROW_KEYS = {
    FIELD_MODEL: "model",
    FIELD_TEMP: "temperature",
    FIELD_REASONING: "reasoning_effort",
    FIELD_FORMAT: "response_format_type",
    FIELD_TOKENS: "max_completion_tokens",
}

# Tipo por defecto si ningún bot del ambiente tiene el campo con valor.
DEFAULT_TYPES = {
    FIELD_MODEL: "S",
    FIELD_TEMP: "N",
    FIELD_REASONING: "S",
    FIELD_FORMAT: "S",
    FIELD_TOKENS: "N",
    FIELD_VARS: "L",
}

# Claves de primer nivel que pueden ser un prompt (además de AGENT_CONFIG).
ROOT_PROMPT_KEYS = ("PROMPT_ORCHESTRATOR",)
ROOT_GROUP = "(raíz)"
LABEL_SEP = " › "

ITEM_KEYS = ("client", "bot")
ITEM_HASH_FIELD = "_item_hash"

_PLACEHOLDER_RE = re.compile(r"\{([A-Za-z_][A-Za-z0-9_]*)\}")


# --------------------------------------------------------------------------
# Utilidades de DynamoDB JSON
# --------------------------------------------------------------------------

def attr_type(attr):
    """Devuelve el tipo DynamoDB ('S', 'N', 'L', ...) de un atributo, o None."""
    if isinstance(attr, dict) and len(attr) == 1:
        return next(iter(attr))
    return None


def get_attr_val(attr_dict):
    """Extrae el valor de un atributo DynamoDB JSON como texto (o lista de textos para L)."""
    t = attr_type(attr_dict)
    if t is None:
        return ""
    val = attr_dict[t]
    if t in ("S", "N"):
        return "" if val is None else str(val)
    if t == "BOOL":
        return "true" if val else "false"
    if t == "NULL":
        return ""
    if t == "L":
        if not isinstance(val, list):
            return []
        out = []
        for x in val:
            xt = attr_type(x)
            out.append(str(x[xt]) if xt in ("S", "N") else json.dumps(x, ensure_ascii=False, sort_keys=True))
        return out
    return json.dumps(val, ensure_ascii=False, sort_keys=True)


def _stable_hash(obj):
    raw = json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def compute_prompts_hash(prompts_map):
    """Hash estable del mapa de prompts (lo usa el flujo de escritura anterior; se reemplaza en la etapa 6)."""
    return _stable_hash(prompts_map)


def editable_fingerprint(fields_map):
    """Hash de los valores crudos (con tipo) de los campos editables de un prompt.
    Sirve para detectar si alguien cambió esos campos desde que se cargaron."""
    subset = {f: fields_map[f] for f in EDITABLE_FIELDS if f in fields_map}
    return _stable_hash(subset)


def template_placeholders(template):
    """Nombres usados como {NOMBRE} en el TEMPLATE (ignora llaves escapadas {{ }})."""
    if not isinstance(template, str) or not template:
        return []
    cleaned = template.replace("{{", "").replace("}}", "")
    return sorted(set(_PLACEHOLDER_RE.findall(cleaned)))


def template_placeholder_counts(template):
    """Frecuencia de cada variable {NOMBRE} en el TEMPLATE."""
    if not isinstance(template, str) or not template:
        return {}
    cleaned = template.replace("{{", "").replace("}}", "")
    return dict(Counter(_PLACEHOLDER_RE.findall(cleaned)))


def template_placeholder_locations(template, max_per_var=20):
    """Ubicaciones (número de línea y fragmento) de cada variable en el TEMPLATE."""
    if not isinstance(template, str) or not template:
        return {}
    lines = template.splitlines()
    locs = {}
    for line_idx, line in enumerate(lines, 1):
        cleaned = line.replace("{{", "").replace("}}", "")
        found = _PLACEHOLDER_RE.findall(cleaned)
        if not found:
            continue
        trimmed = line.strip()
        if len(trimmed) > 160:
            trimmed = trimmed[:157] + "..."
        for v in found:
            if v not in locs:
                locs[v] = []
            if len(locs[v]) < max_per_var:
                locs[v].append({"line": line_idx, "snippet": trimmed})
    return locs


def _key_str(item, key):
    attr = item.get(key)
    if isinstance(attr, dict):
        return str(attr.get("S") or attr.get("N") or "")
    return str(attr or "")


# --------------------------------------------------------------------------
# Descubrimiento de prompts dentro de un ítem
# --------------------------------------------------------------------------

def find_prompt_maps(item):
    """
    Genera (path, group, prompt_name, fields_map) por cada prompt del ítem.
    `path` es la ruta de atributos sin los 'M' intermedios, lista para UpdateItem.
    """
    agent_cfg = item.get("AGENT_CONFIG")
    ac_map = agent_cfg.get("M") if attr_type(agent_cfg) == "M" else None
    if isinstance(ac_map, dict):
        # 1) AGENT_CONFIG.PROMPTS.<X>
        prompts = ac_map.get("PROMPTS")
        if attr_type(prompts) == "M":
            for name in sorted(prompts["M"]):
                p = prompts["M"][name]
                if attr_type(p) == "M":
                    yield ["AGENT_CONFIG", "PROMPTS", name], "", name, p["M"]
        # 2) AGENT_CONFIG.<GRUPO>.PROMPTS.<X>
        for group in sorted(ac_map):
            if group == "PROMPTS":
                continue
            g = ac_map[group]
            if attr_type(g) != "M":
                continue
            g_prompts = g["M"].get("PROMPTS")
            if attr_type(g_prompts) != "M":
                continue
            for name in sorted(g_prompts["M"]):
                p = g_prompts["M"][name]
                if attr_type(p) == "M":
                    yield ["AGENT_CONFIG", group, "PROMPTS", name], group, name, p["M"]
    # 3) Prompts de primer nivel (fuera de AGENT_CONFIG)
    for key in ROOT_PROMPT_KEYS:
        p = item.get(key)
        if attr_type(p) == "M" and FIELD_MODEL in p["M"]:
            yield [key], ROOT_GROUP, key, p["M"]
    for key, val in item.items():
        if key in ("AGENT_CONFIG", ITEM_KEYS[0], ITEM_KEYS[1], ITEM_HASH_FIELD) or key in ROOT_PROMPT_KEYS:
            continue
        if attr_type(val) == "M":
            v_map = val["M"]
            p_map = v_map.get("PROMPTS")
            if attr_type(p_map) == "M":
                for name in sorted(p_map["M"]):
                    p = p_map["M"][name]
                    if attr_type(p) == "M":
                        yield [key, "PROMPTS", name], key, name, p["M"]


def prompt_label(group, name):
    if not group:
        return name
    if group == ROOT_GROUP:
        return f"{ROOT_GROUP} {name}"
    return f"{group}{LABEL_SEP}{name}"


def row_id(client, bot, path):
    return f"{client}/{bot}::{'.'.join(path)}"


# --------------------------------------------------------------------------
# Lista plana e índices
# --------------------------------------------------------------------------

def extract_flat_rows(items):
    """Convierte ítems de DynamoDB en una lista plana: una fila por cada pareja (bot + prompt)."""
    rows = []
    for item in items:
        if not isinstance(item, dict):
            continue
        client, bot = _key_str(item, ITEM_KEYS[0]), _key_str(item, ITEM_KEYS[1])
        if not client or not bot:
            continue
        item_hash = _key_str(item, ITEM_HASH_FIELD)

        for path, group, name, fields in find_prompt_maps(item):
            row = {
                "id": row_id(client, bot, path),
                "client": client,
                "bot": bot,
                "client_bot": f"{client} / {bot}",
                "group": group,
                "prompt_name": name,
                "prompt": prompt_label(group, name),
                "path": path,
                "present": {},
                "types": {},
            }
            for field in EDITABLE_FIELDS:
                attr = fields.get(field)
                row["present"][field] = field in fields
                row["types"][field] = attr_type(attr)
            for field, key in ROW_KEYS.items():
                val = get_attr_val(fields.get(field))
                val_str = val if isinstance(val, str) else json.dumps(val, ensure_ascii=False)
                row[key] = val_str
                row[field] = val_str
            vars_val = get_attr_val(fields.get(FIELD_VARS))
            row["input_variables"] = vars_val if isinstance(vars_val, list) else []
            row[FIELD_VARS] = row["input_variables"]
            tpl = fields.get(FIELD_TEMPLATE) or fields.get("PROMPT") or fields.get("SYSTEM_PROMPT") or fields.get("TEXT")
            tpl_text = ""
            if tpl:
                t = attr_type(tpl)
                if t == "S":
                    tpl_text = tpl.get("S") or ""
                else:
                    raw_val = get_attr_val(tpl)
                    tpl_text = raw_val if isinstance(raw_val, str) else json.dumps(raw_val, ensure_ascii=False)
            char_count = len(tpl_text)
            row["char_count"] = char_count
            row["estimated_tokens"] = round(char_count / 4) if char_count else 0
            row["template_placeholders"] = template_placeholders(tpl_text)
            var_counts = template_placeholder_counts(tpl_text)
            row["variable_counts"] = var_counts
            repeated = sorted(
                [{"name": k, "count": v} for k, v in var_counts.items() if v > 1],
                key=lambda x: x["count"],
                reverse=True
            )
            row["repeated_vars"] = repeated
            row["repeated_vars_total"] = sum(v["count"] for v in repeated)
            if repeated:
                all_locs = template_placeholder_locations(tpl_text)
                rep_names = {v["name"] for v in repeated}
                row["variable_locations"] = {k: all_locs[k] for k in rep_names if k in all_locs}
            else:
                row["variable_locations"] = {}
            row["fingerprint"] = editable_fingerprint(fields)
            row["item_hash"] = item_hash
            rows.append(row)
    return rows


def compute_type_stats(rows):
    """
    Por cada campo editable: cuántas veces aparece cada tipo (separando vacíos) y
    qué tipo usar si hay que escribir un valor en un prompt que no lo tiene.
    El tipo preferido es el mayoritario entre los valores NO vacíos.
    """
    stats = {}
    for field in EDITABLE_FIELDS:
        counts = Counter()
        non_empty = Counter()
        key = ROW_KEYS.get(field)
        for r in rows:
            t = r["types"].get(field)
            if not t:
                counts["(ausente)"] += 1
                continue
            empty = (r["input_variables"] == []) if field == FIELD_VARS else (r.get(key, "") == "")
            counts[t + (" vacío" if empty else "")] += 1
            if not empty:
                non_empty[t] += 1
        preferred = non_empty.most_common(1)[0][0] if non_empty else DEFAULT_TYPES[field]
        stats[field] = {"counts": dict(counts), "preferred": preferred}
    return stats


def build_indices(rows):
    """Índices en memoria: prompt -> filas, modelo -> filas, y familias (clientes)."""
    by_prompt, by_model = {}, {}
    all_bots, families = set(), Counter()

    for r in rows:
        all_bots.add(r["client_bot"])
        p = by_prompt.setdefault(r["prompt"], {
            "bots": set(),
            "models": set(),
            "model_counts": Counter(),
            "row_ids": [],
            "group": r["group"],
            "prompt_name": r["prompt_name"]
        })
        p["bots"].add(r["client_bot"])
        if r["model"]:
            p["models"].add(r["model"])
            p["model_counts"][r["model"]] += 1
        p["row_ids"].append(r["id"])

        if r["model"]:
            m = by_model.setdefault(r["model"], {"bots": set(), "prompts": set(), "row_ids": []})
            m["bots"].add(r["client_bot"])
            m["prompts"].add(r["prompt"])
            m["row_ids"].append(r["id"])

    for bot in all_bots:
        families[bot.split(" / ", 1)[0]] += 1

    prompts_index = [{
        "prompt": name,
        "prompt_name": info.get("prompt_name") or name,
        "group": info["group"],
        "bot_count": len(info["bots"]),
        "row_count": len(info["row_ids"]),
        "model_count": len(info["models"]),
        "models": sorted(info["models"]),
        "majority_model": info["model_counts"].most_common(1)[0][0] if info.get("model_counts") else (sorted(info["models"])[0] if info["models"] else ""),
        "row_ids": info["row_ids"],
    } for name, info in sorted(by_prompt.items())]

    models_index = [{
        "model": name,
        "prompt_count": len(info["row_ids"]),
        "bot_count": len(info["bots"]),
        "prompts": sorted(info["prompts"]),
        "row_ids": info["row_ids"],
    } for name, info in sorted(by_model.items())]

    return {
        "prompts_index": prompts_index,
        "models_index": models_index,
        "families": dict(sorted(families.items())),
        "total_bots": len(all_bots),
        "total_prompts": len(by_prompt),
        "total_rows": len(rows),
        "rows_without_model": sum(1 for r in rows if not r["model"]),
        "all_models": sorted(by_model),
    }


# --------------------------------------------------------------------------
# Lectura de DynamoDB
# --------------------------------------------------------------------------

def projection_kwargs():
    """Proyección: claves, AGENT_CONFIG (para descubrir grupos nuevos), prompts raíz y _item_hash."""
    names = {"#k0": ITEM_KEYS[0], "#k1": ITEM_KEYS[1], "#ac": "AGENT_CONFIG", "#ih": ITEM_HASH_FIELD}
    parts = ["#k0", "#k1", "#ac", "#ih"]
    for i, key in enumerate(ROOT_PROMPT_KEYS):
        names[f"#r{i}"] = key
        parts.append(f"#r{i}")
    return {"ProjectionExpression": ", ".join(parts), "ExpressionAttributeNames": names}


def scan_environment_prompts(dynamo_client_instance, table_name):
    """Scan proyectado y paginado (cliente boto3 de bajo nivel, DynamoDB JSON)."""
    items = []
    kwargs = {"TableName": table_name, **projection_kwargs()}
    while True:
        resp = dynamo_client_instance.scan(**kwargs)
        items.extend(resp.get("Items", []))
        if "LastEvaluatedKey" not in resp:
            break
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
    return items


def load_environment(dynamo_client_instance, table_name, env=""):
    """Carga un ambiente completo y arma filas, índices y estadísticas de tipos."""
    t0 = time.time()
    items = scan_environment_prompts(dynamo_client_instance, table_name)
    rows = extract_flat_rows(items)
    result = {"env": env, "rows": rows, **build_indices(rows), "type_stats": compute_type_stats(rows)}
    result["total_items"] = len(items)
    result["load_time_seconds"] = round(time.time() - t0, 2)
    return result


def _print_summary(data):
    print(f"Ambiente {data['env']}: {data['total_items']} ítems, {data['total_bots']} bots con prompts, "
          f"{data['total_rows']} filas, {data['total_prompts']} prompts distintos, "
          f"{len(data['all_models'])} modelos. Cargado en {data['load_time_seconds']} s.")
    print("\nModelos:")
    for m in data["models_index"]:
        print(f"  {m['model']:<24} {m['prompt_count']:>4} prompts · {m['bot_count']:>3} bots")
    print("\nPrompts:")
    for p in data["prompts_index"]:
        print(f"  {p['prompt']:<52} {p['bot_count']:>3} bots · {p['model_count']} modelos")
    print("\nTipos por campo (preferido = el que se usa si el campo no existe):")
    for field, s in data["type_stats"].items():
        print(f"  {field:<22} preferido={s['preferred']:<2} {s['counts']}")
    if data["rows_without_model"]:
        print(f"\nFilas sin OPENAI_MODEL: {data['rows_without_model']}")


if __name__ == "__main__":
    from promote_config import ENVS, client_for

    env_arg = sys.argv[1] if len(sys.argv) > 1 else "desa"
    if env_arg not in ENVS:
        sys.exit(f"Ambiente inválido: {env_arg}. Opciones: {', '.join(ENVS)}")
    _print_summary(load_environment(client_for(env_arg), ENVS[env_arg]["table"], env_arg))
