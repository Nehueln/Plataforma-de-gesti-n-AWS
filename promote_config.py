#!/usr/bin/env python3
"""
Promueve configuraciones (prompts) de clientes entre tablas DynamoDB.

Ejemplos:
  # Ver diferencias desa -> test de credcor/deuda (no escribe nada)
  python promote_config.py desa test --client credcor --bot deuda

  # Todos los bots de varios clientes, modo interactivo
  python promote_config.py desa test --client credcor otro --apply

  # Todo lo de la tabla origen, aplicar sin preguntar (cuidado)
  python promote_config.py test prod --all --apply --yes

  # Listar client/bot de la tabla origen
  python promote_config.py desa test --list
"""
import argparse
import copy
import datetime
import json
import pathlib
import sys
from decimal import Decimal

import boto3

# ----------------------------------------------------------------------------
# CONFIGURACION: ajustar a tu entorno
# ----------------------------------------------------------------------------
KEYS = ("client", "bot")  # partition key + sort key de tus tablas

ENVS = {
    "desa": {"table": "desa_sonia_config", "profile": None, "region": "us-east-1"},
    "test": {"table": "test_sonia_config", "profile": None, "region": "us-east-1"},
    "prod": {"table": "prod_sonia_config", "profile": None, "region": "us-east-1"},
}
# Si cada ambiente vive en otra cuenta AWS, poné el profile de cada uno
# (ej: "profile": "mi-perfil-prod").

# Campos que NUNCA se comparan/copian (propios de cada ambiente)
IGNORE_PREFIXES = ["updated_at", "created_at", "model"]  # "model" queda para la etapa 2
# ----------------------------------------------------------------------------

RED, GREEN, YELLOW, RESET = "\033[31m", "\033[32m", "\033[33m", "\033[0m"


def table_for(env):
    cfg = ENVS[env]
    session = boto3.Session(profile_name=cfg["profile"], region_name=cfg["region"])
    return session.resource("dynamodb").Table(cfg["table"])


def client_for(env):
    cfg = ENVS[env]
    session = boto3.Session(profile_name=cfg["profile"], region_name=cfg["region"])
    return session.client("dynamodb")


def get_item(table, key):
    return table.get_item(Key=dict(zip(KEYS, key))).get("Item")


def scan_keys(table):
    """Devuelve lista ordenada de tuplas (client, bot)."""
    keys = []
    kwargs = {"ProjectionExpression": "#c, #b",
              "ExpressionAttributeNames": {"#c": KEYS[0], "#b": KEYS[1]}}
    while True:
        resp = table.scan(**kwargs)
        keys += [(i[KEYS[0]], i[KEYS[1]]) for i in resp["Items"]]
        if "LastEvaluatedKey" not in resp:
            return sorted(keys)
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]


def jdump(v):
    def default(o):
        return float(o) if isinstance(o, Decimal) and o % 1 else int(o) if isinstance(o, Decimal) else str(o)
    return json.dumps(v, ensure_ascii=False, indent=2, default=default)


def flatten(d, prefix=()):
    """Aplana dicts anidados a {ruta(tupla): valor}. Listas y valores = hojas."""
    out = {}
    for k, v in d.items():
        path = prefix + (k,)
        if isinstance(v, dict) and v:
            out.update(flatten(v, path))
        else:
            out[path] = v
    return out


def ignored(path, only):
    dotted = ".".join(path)
    if any(dotted == p or dotted.startswith(p + ".") for p in IGNORE_PREFIXES):
        return True
    if only and not any(dotted == p or dotted.startswith(p + ".") for p in only):
        return True
    return False


def diff(src, dst, only):
    fs, fd = flatten(src), flatten(dst)
    changes = []
    for path in sorted(set(fs) | set(fd)):
        if path[0] in KEYS or ignored(path, only):
            continue
        if path not in fd:
            changes.append((path, None, fs[path], "nuevo"))
        elif path not in fs:
            changes.append((path, fd[path], None, "eliminado"))
        elif fs[path] != fd[path]:
            changes.append((path, fd[path], fs[path], "modificado"))
    return changes


def set_path(d, path, value):
    for k in path[:-1]:
        d = d.setdefault(k, {})
    d[path[-1]] = value


def del_path(d, path):
    for k in path[:-1]:
        d = d.get(k, {})
    d.pop(path[-1], None)


def show(change):
    path, old, new, kind = change
    print(f"\n{YELLOW}[{kind}] {'.'.join(path)}{RESET}")
    if kind != "nuevo":
        print(f"{RED}- (destino){RESET}\n{RED}{jdump(old)}{RESET}")
    if kind != "eliminado":
        print(f"{GREEN}+ (origen){RESET}\n{GREEN}{jdump(new)}{RESET}")


def backup(env, key, item):
    folder = pathlib.Path("backups") / env
    folder.mkdir(parents=True, exist_ok=True)
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    f = folder / f"{key[0]}_{key[1]}_{ts}.json"
    f.write_text(jdump(item), encoding="utf-8")
    return f


def process(key, src_t, dst_t, args):
    print(f"\n{'=' * 70}\nclient={key[0]}  bot={key[1]}  ({args.src} -> {args.dst})\n{'=' * 70}")
    src, dst = get_item(src_t, key), get_item(dst_t, key)
    if not src:
        print(f"  No existe en {args.src}, se omite.")
        return
    if not dst:
        print(f"  No existe en {args.dst}. Se crearía completo.")
        if args.apply and (args.yes or input("  ¿Crear? [y/N] ").lower() == "y"):
            new = {k: v for k, v in src.items() if k in KEYS or not ignored((k,), args.only)}
            new.update(dict(zip(KEYS, key)))
            dst_t.put_item(Item=new)
            print("  Creado.")
        return

    changes = diff(src, dst, args.only)
    if not changes:
        print("  Sin diferencias.")
        return

    result, apply_all = copy.deepcopy(dst), args.yes
    applied = 0
    for ch in changes:
        show(ch)
        if not args.apply:
            continue
        if not apply_all:
            ans = input("¿Aplicar? [y]es / [n]o / [a]ll restantes / [q]uit cliente: ").lower()
            if ans == "q":
                break
            if ans == "a":
                apply_all = True
            elif ans != "y":
                continue
        path, _, new, kind = ch
        del_path(result, path) if kind == "eliminado" else set_path(result, path, new)
        applied += 1

    if args.apply and applied:
        print(f"\nBackup destino: {backup(args.dst, key, dst)}")
        dst_t.put_item(Item=result)
        print(f"{GREEN}Aplicados {applied} cambios en {args.dst}/{key[0]}/{key[1]}{RESET}")
    elif not args.apply:
        print(f"\n({len(changes)} diferencias; modo lectura, usá --apply para escribir)")


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument("src", choices=ENVS)
    p.add_argument("dst", choices=ENVS)
    p.add_argument("--client", nargs="+", help="uno o más client (ej: credcor)")
    p.add_argument("--bot", nargs="+", help="uno o más bot (ej: deuda). Si se omite: todos los bots del client")
    p.add_argument("--all", action="store_true", help="todos los client/bot de la tabla origen")
    p.add_argument("--only", nargs="+", help="limitar a estos campos (ej: prompts prompts.system)")
    p.add_argument("--apply", action="store_true", help="escribir cambios (sin esto solo muestra diff)")
    p.add_argument("--yes", action="store_true", help="no preguntar, aplicar todo")
    p.add_argument("--list", action="store_true", help="listar clientes del origen y salir")
    args = p.parse_args()

    if args.src == args.dst:
        sys.exit("Origen y destino no pueden ser iguales.")

    src_t, dst_t = table_for(args.src), table_for(args.dst)

    if args.list:
        print("\n".join(f"{c} / {b}" for c, b in scan_keys(src_t)))
        return

    if not (args.all or args.client):
        sys.exit("Indicá --client X [Y ...] [--bot Z] o --all")
    keys = scan_keys(src_t)
    if not args.all:
        keys = [k for k in keys if k[0] in args.client and (not args.bot or k[1] in args.bot)]
    if not keys:
        sys.exit("No se encontró ningún client/bot en el origen con ese filtro.")

    if args.dst == "prod" and args.apply:
        if input("Vas a escribir en PROD. Escribí 'prod' para continuar: ") != "prod":
            sys.exit("Cancelado.")

    for k in keys:
        process(k, src_t, dst_t, args)


if __name__ == "__main__":
    main()