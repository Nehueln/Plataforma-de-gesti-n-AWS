#!/usr/bin/env python3
"""
Etapa 1: carga, lista plana (bot + prompt), índices y tipos.
Usa ítems sintéticos y, además, los ítems reales que hay en backups/.
"""
import glob
import json
import os
import sys
import unittest
from collections import defaultdict
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import models_manager as mm  # noqa: E402

BACKUPS = os.path.join(os.path.dirname(__file__), "..", "backups")


def prompt(model, temp=None, reasoning=None, fmt=None, tokens=None, variables=None, template=""):
    m = {"OPENAI_MODEL": {"S": model}, "TEMPLATE": {"S": template}}
    if temp is not None:
        m["TEMPERATURE"] = {"N": temp}
    if reasoning is not None:
        m["REASONING_EFFORT"] = {"S": reasoning}
    if fmt is not None:
        m["RESPONSE_FORMAT_TYPE"] = {"S": fmt}
    if tokens is not None:
        m["MAX_COMPLETION_TOKENS"] = tokens
    if variables is not None:
        m["INPUT_VARIABLE"] = {"L": [{"S": v} for v in variables]}
    return {"M": m}


def item(client, bot, prompts, groups=None, root=None, item_hash=None):
    ac = {"PROMPTS": {"M": prompts}, "TTL_SECONDS": {"N": "300"}}
    for g, gp in (groups or {}).items():
        ac[g] = {"M": {"PROMPTS": {"M": gp}, "ENABLED": {"BOOL": True}}}
    it = {"client": {"S": client}, "bot": {"S": bot}, "AGENT_CONFIG": {"M": ac}}
    if root:
        it["PROMPT_ORCHESTRATOR"] = root
    if item_hash:
        it["_item_hash"] = {"S": item_hash}
    return it


def latest_backups_by_env():
    """Último backup de cada (ambiente, client, bot)."""
    latest = {}
    for f in sorted(glob.glob(os.path.join(BACKUPS, "*.json"))):
        with open(f, encoding="utf-8") as fh:
            data = json.load(fh)
        if not isinstance(data, dict):
            continue
        env = os.path.basename(f).split("_", 1)[0]
        c_obj, b_obj = data.get("client"), data.get("bot")
        c = c_obj.get("S") if isinstance(c_obj, dict) else None
        b = b_obj.get("S") if isinstance(b_obj, dict) else None
        if c and b:
            latest[(env, c, b)] = data
    by_env = defaultdict(list)
    for (env, _, _), data in latest.items():
        by_env[env].append(data)
    return by_env


def sample_items():
    return [
        item("corrientes", "chatbot", {
            "PROMPT_CHANGE_TOPIC": prompt("gpt-6.1-sol", "1", "medium", "text", {"N": "2000"},
                                          ["CONVERSATION", "DATETIME"], "Hola {CONVERSATION} {{literal}} {DATETIME}"),
            "PROMPT_ORCHESTRATOR": prompt("gpt-6-luna", "0", "", None, {"S": ""}, ["QUERY"]),
        }, groups={
            "SENTIMENT_ANALYSIS": {"PROMPT_INTENTION": prompt("gpt-6.1-sol", "0", variables=["CONVERSATION"])},
        }, root={"M": {"OPENAI_MODEL": {"S": "gpt-4o"}, "TEMPLATE": {"S": "x"}, "INPUT_VARIABLE": {"L": []}}},
            item_hash="h1"),
        item("liderazgo", "tru", {
            "PROMPT_CHANGE_TOPIC": prompt("gpt-6-luna", "1", variables=["CONVERSATION"]),
        }),
        {"client": {"S": "sin"}, "bot": {"S": "prompts"}},  # ítem sin AGENT_CONFIG: se ignora
        {"bot": {"S": "sin-client"}, "AGENT_CONFIG": {"M": {}}},  # sin clave: se ignora
    ]


class TestDiscoveryAndRows(unittest.TestCase):

    def setUp(self):
        self.items = sample_items()
        self.rows = mm.extract_flat_rows(self.items)
        self.by_id = {r["id"]: r for r in self.rows}

    def test_finds_all_four_locations(self):
        self.assertEqual(len(self.rows), 5)
        labels = sorted(r["prompt"] for r in self.rows if r["client"] == "corrientes")
        self.assertEqual(labels, [
            "(raíz) PROMPT_ORCHESTRATOR",
            "PROMPT_CHANGE_TOPIC",
            "PROMPT_ORCHESTRATOR",
            "SENTIMENT_ANALYSIS › PROMPT_INTENTION",
        ])

    def test_paths_are_update_ready(self):
        r = self.by_id["corrientes/chatbot::AGENT_CONFIG.SENTIMENT_ANALYSIS.PROMPTS.PROMPT_INTENTION"]
        self.assertEqual(r["path"], ["AGENT_CONFIG", "SENTIMENT_ANALYSIS", "PROMPTS", "PROMPT_INTENTION"])
        self.assertEqual(r["group"], "SENTIMENT_ANALYSIS")
        self.assertEqual(r["prompt_name"], "PROMPT_INTENTION")
        root = self.by_id["corrientes/chatbot::PROMPT_ORCHESTRATOR"]
        self.assertEqual(root["path"], ["PROMPT_ORCHESTRATOR"])
        self.assertEqual(root["model"], "gpt-4o")

    def test_values_types_and_presence(self):
        r = self.by_id["corrientes/chatbot::AGENT_CONFIG.PROMPTS.PROMPT_CHANGE_TOPIC"]
        self.assertEqual((r["model"], r["temperature"], r["reasoning_effort"], r["response_format_type"],
                          r["max_completion_tokens"]), ("gpt-6.1-sol", "1", "medium", "text", "2000"))
        self.assertEqual(r["types"]["MAX_COMPLETION_TOKENS"], "N")
        self.assertEqual(r["input_variables"], ["CONVERSATION", "DATETIME"])
        self.assertEqual(r["template_placeholders"], ["CONVERSATION", "DATETIME"])
        self.assertEqual(r["item_hash"], "h1")

        o = self.by_id["corrientes/chatbot::AGENT_CONFIG.PROMPTS.PROMPT_ORCHESTRATOR"]
        self.assertEqual(o["max_completion_tokens"], "")
        self.assertEqual(o["types"]["MAX_COMPLETION_TOKENS"], "S")
        self.assertFalse(o["present"]["RESPONSE_FORMAT_TYPE"])
        self.assertIsNone(o["types"]["RESPONSE_FORMAT_TYPE"])
        self.assertEqual(o["response_format_type"], "")

    def test_template_is_not_sent(self):
        for r in self.rows:
            self.assertNotIn("template", r)
            self.assertNotIn("TEMPLATE", json.dumps(r))

    def test_fingerprint_changes_only_with_editable_fields(self):
        base = mm.extract_flat_rows([self.items[1]])[0]["fingerprint"]
        changed_tpl = json.loads(json.dumps(self.items[1]))
        changed_tpl["AGENT_CONFIG"]["M"]["PROMPTS"]["M"]["PROMPT_CHANGE_TOPIC"]["M"]["TEMPLATE"] = {"S": "otro"}
        self.assertEqual(mm.extract_flat_rows([changed_tpl])[0]["fingerprint"], base)
        changed_type = json.loads(json.dumps(self.items[1]))
        changed_type["AGENT_CONFIG"]["M"]["PROMPTS"]["M"]["PROMPT_CHANGE_TOPIC"]["M"]["TEMPERATURE"] = {"S": "1"}
        self.assertNotEqual(mm.extract_flat_rows([changed_type])[0]["fingerprint"], base)


class TestIndices(unittest.TestCase):

    def setUp(self):
        self.rows = mm.extract_flat_rows(sample_items())
        self.idx = mm.build_indices(self.rows)

    def test_prompt_index(self):
        p = {x["prompt"]: x for x in self.idx["prompts_index"]}
        self.assertEqual(p["PROMPT_CHANGE_TOPIC"]["bot_count"], 2)
        self.assertEqual(p["PROMPT_CHANGE_TOPIC"]["models"], ["gpt-6-luna", "gpt-6.1-sol"])
        self.assertEqual(p["PROMPT_CHANGE_TOPIC"]["model_count"], 2)
        self.assertEqual(self.idx["total_prompts"], 4)

    def test_model_index_spans_prompts_and_groups(self):
        m = {x["model"]: x for x in self.idx["models_index"]}
        sol = m["gpt-6.1-sol"]
        self.assertEqual(sol["prompt_count"], 2)
        self.assertEqual(sol["bot_count"], 1)
        self.assertEqual(sol["prompts"], ["PROMPT_CHANGE_TOPIC", "SENTIMENT_ANALYSIS › PROMPT_INTENTION"])
        self.assertEqual([x["model"] for x in self.idx["models_index"]], sorted(m))  # alfabético

    def test_families_and_totals(self):
        self.assertEqual(self.idx["families"], {"corrientes": 1, "liderazgo": 1})
        self.assertEqual(self.idx["total_bots"], 2)
        self.assertEqual(self.idx["total_rows"], 5)

    def test_type_stats_prefer_non_empty_majority(self):
        stats = mm.compute_type_stats(self.rows)
        self.assertEqual(stats["MAX_COMPLETION_TOKENS"]["preferred"], "N")
        self.assertEqual(stats["TEMPERATURE"]["preferred"], "N")
        self.assertEqual(stats["RESPONSE_FORMAT_TYPE"]["preferred"], "S")


class TestScan(unittest.TestCase):

    def test_projection_and_pagination(self):
        items = sample_items()
        client = MagicMock()
        client.scan.side_effect = [
            {"Items": items[:1], "LastEvaluatedKey": {"client": {"S": "x"}}},
            {"Items": items[1:]},
        ]
        data = mm.load_environment(client, "desa_sonia_config", "desa")
        self.assertEqual(client.scan.call_count, 2)
        first, second = client.scan.call_args_list
        self.assertEqual(first.kwargs["TableName"], "desa_sonia_config")
        self.assertNotIn("ExclusiveStartKey", first.kwargs)
        self.assertEqual(second.kwargs["ExclusiveStartKey"], {"client": {"S": "x"}})
        names = set(first.kwargs["ExpressionAttributeNames"].values())
        self.assertEqual(names, {"client", "bot", "AGENT_CONFIG", "_item_hash", "PROMPT_ORCHESTRATOR"})
        self.assertNotIn("API_KEYS", json.dumps(first.kwargs))
        self.assertEqual(data["total_rows"], 5)
        self.assertEqual(data["total_items"], 4)

    def test_compare_ui_endpoint_uses_cache(self):
        import Compare_ui
        client = MagicMock()
        client.scan.return_value = {"Items": sample_items()}
        with patch("Compare_ui.dynamo_client", return_value=client):
            Compare_ui._models_data_cache.pop("desa", None)
            a = Compare_ui.get_models_view_data("desa", refresh=True)
            b = Compare_ui.get_models_view_data("desa")
            self.assertIs(a, b)
            self.assertEqual(client.scan.call_count, 1)
            Compare_ui.get_models_view_data("desa", refresh=True)
            self.assertEqual(client.scan.call_count, 2)
        Compare_ui._models_data_cache.pop("desa", None)


class TestRealBackups(unittest.TestCase):
    """Datos reales: último backup de cada bot por ambiente."""

    @classmethod
    def setUpClass(cls):
        cls.by_env = latest_backups_by_env()
        if not cls.by_env:
            raise unittest.SkipTest("No hay backups reales en backups/")

    def test_every_prompt_with_model_becomes_a_row(self):
        for env, items in self.by_env.items():
            rows = mm.extract_flat_rows(items)
            expected = 0
            for it in items:
                expected += sum(1 for _ in mm.find_prompt_maps(it))
            self.assertEqual(len(rows), expected, env)
            ids = [r["id"] for r in rows]
            self.assertEqual(len(ids), len(set(ids)), f"ids repetidos en {env}")

    def test_no_model_outside_discovered_locations(self):
        """Ningún OPENAI_MODEL del ítem queda fuera de las filas."""
        def count_models(v):
            if isinstance(v, dict):
                return (1 if "OPENAI_MODEL" in v else 0) + sum(count_models(x) for x in v.values())
            if isinstance(v, list):
                return sum(count_models(x) for x in v)
            return 0
        for env, items in self.by_env.items():
            for it in items:
                n_rows = len(mm.extract_flat_rows([it]))
                self.assertEqual(n_rows, count_models(it), f"{env} {it['client']['S']}/{it['bot']['S']}")

    def test_real_types_match_assumptions(self):
        all_rows = [r for items in self.by_env.values() for r in mm.extract_flat_rows(items)]
        stats = mm.compute_type_stats(all_rows)
        self.assertEqual(stats["MAX_COMPLETION_TOKENS"]["preferred"], "N")
        self.assertEqual(stats["TEMPERATURE"]["preferred"], "N")
        self.assertEqual(stats["RESPONSE_FORMAT_TYPE"]["preferred"], "S")
        self.assertEqual(stats["OPENAI_MODEL"]["counts"].get("S", 0), len(all_rows))
        idx = mm.build_indices(all_rows)
        print(f"\n[Datos reales] {sum(len(v) for v in self.by_env.values())} ítems -> {len(all_rows)} filas, "
              f"{idx['total_prompts']} prompts, {len(idx['all_models'])} modelos.")
        for f, s in stats.items():
            print(f"   {f:<22} preferido={s['preferred']} {s['counts']}")


if __name__ == "__main__":
    unittest.main()
