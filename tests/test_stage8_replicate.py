import unittest
from unittest.mock import MagicMock, patch
import json

import Compare_ui


class TestStage8Replicate(unittest.TestCase):

    def test_replicate_ui_elements_present(self):
        """Verifica la presencia en el HTML del botón de replicar y el modal correspondiente."""
        html = Compare_ui.HTML

        # Botón en barra superior
        self.assertIn('id="btnReplicatePrompts"', html)
        self.assertIn('Replicar estructura de', html)

        # Modal y controles
        self.assertIn('id="modelReplicateModal"', html)
        self.assertIn('id="replicateSourceEnvSelect"', html)
        self.assertIn('id="modalReplicateTargetBadge"', html)
        self.assertIn('id="modalReplicateCounts"', html)
        self.assertIn('id="btnRefreshReplicatePlan"', html)
        self.assertIn('id="statRepChangedBots"', html)
        self.assertIn('id="statRepChangedPrompts"', html)
        self.assertIn('id="statRepUnchangedBots"', html)
        self.assertIn('id="statRepSkippedBots"', html)
        self.assertIn('id="modalReplicateProdWarning"', html)
        self.assertIn('id="modalReplicateProdInput"', html)
        self.assertIn('id="replicateTableFilter"', html)
        self.assertIn('id="modalReplicateTableBody"', html)
        self.assertIn('id="btnModalReplicateCancel"', html)
        self.assertIn('id="btnModalReplicateConfirm"', html)

        # Funciones JS
        self.assertIn('openModelReplicateModal', html)
        self.assertIn('closeModelReplicateModal', html)
        self.assertIn('loadReplicatePlan', html)
        self.assertIn('renderReplicateDiffTable', html)
        self.assertIn('applyReplicationPlan', html)
        self.assertIn('/api/models/replicate-plan', html)

    def test_compute_replicate_plan_validation(self):
        """Verifica validación de ambientes de origen y destino."""
        with self.assertRaises(ValueError):
            Compare_ui.compute_replicate_plan("invalid_env", "desa")
        with self.assertRaises(ValueError):
            Compare_ui.compute_replicate_plan("desa", "invalid_env")
        with self.assertRaises(ValueError):
            Compare_ui.compute_replicate_plan("desa", "desa")

    @patch("Compare_ui.get_models_view_data")
    def test_compute_replicate_plan_diffs(self, mock_get_models_view_data):
        """Verifica el cálculo de diffs entre origen y destino."""
        # Bot 1: idéntico en ambos (sin cambios)
        # Bot 2: destino difiere en modelo, temperatura y variables (debe generar cambios)
        # Bot 3: solo en destino (debe ser omitido y registrado en skipped_bots)

        source_rows = [
            {
                "client": "cliente_a", "bot": "bot1",
                "prompt_name": "prompt_saludo", "prompt": "prompt_saludo",
                "path": ["AGENT_CONFIG", "PROMPTS", "prompt_saludo"],
                "model": "gpt-4o", "temperature": "0.7",
                "reasoning_effort": "none", "response_format_type": "json_object",
                "max_completion_tokens": "400", "input_variables": ["nombre"],
                "item_hash": "hash_src_1"
            },
            {
                "client": "cliente_b", "bot": "bot2",
                "prompt_name": "prompt_despedida", "prompt": "prompt_despedida",
                "path": ["AGENT_CONFIG", "PROMPTS", "prompt_despedida"],
                "model": "gpt-4o", "temperature": "0.2",
                "reasoning_effort": "low", "response_format_type": "text",
                "max_completion_tokens": "500", "input_variables": ["nombre", "fecha"],
                "item_hash": "hash_src_2"
            }
        ]

        target_rows = [
            {
                "client": "cliente_a", "bot": "bot1",
                "prompt_name": "prompt_saludo", "prompt": "prompt_saludo",
                "path": ["AGENT_CONFIG", "PROMPTS", "prompt_saludo"],
                "model": "gpt-4o", "temperature": "0.7",
                "reasoning_effort": "none", "response_format_type": "json_object",
                "max_completion_tokens": "400", "input_variables": ["nombre"],
                "item_hash": "hash_tgt_1"
            },
            {
                "client": "cliente_b", "bot": "bot2",
                "prompt_name": "prompt_despedida", "prompt": "prompt_despedida",
                "path": ["AGENT_CONFIG", "PROMPTS", "prompt_despedida"],
                "model": "gpt-4o-mini", "temperature": "1.0",
                "reasoning_effort": "", "response_format_type": "json_object",
                "max_completion_tokens": "300", "input_variables": ["nombre", "dni"],
                "item_hash": "hash_tgt_2"
            },
            {
                "client": "cliente_c", "bot": "bot3_huerfano",
                "prompt_name": "prompt_x", "prompt": "prompt_x",
                "path": ["AGENT_CONFIG", "PROMPTS", "prompt_x"],
                "model": "gpt-4o", "temperature": "0.5",
                "reasoning_effort": "", "response_format_type": "",
                "max_completion_tokens": "", "input_variables": [],
                "item_hash": "hash_tgt_3"
            }
        ]

        def fake_get_view_data(env, refresh=False):
            if env == "prod":
                return {"rows": source_rows}
            elif env == "desa":
                return {"rows": target_rows}
            return {"rows": []}

        mock_get_models_view_data.side_effect = fake_get_view_data

        plan = Compare_ui.compute_replicate_plan("prod", "desa")

        self.assertTrue(plan["ok"])
        self.assertEqual(plan["source_env"], "prod")
        self.assertEqual(plan["target_env"], "desa")
        self.assertEqual(plan["total_target_bots"], 3)
        self.assertEqual(plan["skipped_bots_count"], 1)
        self.assertIn("cliente_c / bot3_huerfano", plan["skipped_bots"])
        self.assertEqual(plan["unchanged_bots_count"], 1)
        self.assertIn("cliente_a / bot1", plan["unchanged_bots"])
        self.assertEqual(plan["changed_bots_count"], 1)
        self.assertEqual(plan["changed_prompts_count"], 1)

        # Verificar contenido de cambios para bot2
        self.assertEqual(len(plan["changes"]), 1)
        change_entry = plan["changes"][0]
        self.assertEqual(change_entry["client"], "cliente_b")
        self.assertEqual(change_entry["bot"], "bot2")
        self.assertEqual(change_entry["expected_hash"], "hash_tgt_2")

        p_change = change_entry["prompts"]["prompt_despedida"]
        self.assertEqual(p_change["model"], "gpt-4o")
        self.assertEqual(p_change["temperature"], "0.2")
        self.assertEqual(p_change["reasoning_effort"], "low")
        self.assertEqual(p_change["response_format_type"], "text")
        self.assertEqual(p_change["max_tokens"], "500")
        self.assertEqual(p_change["set_variables"], ["nombre", "fecha"])
        self.assertEqual(p_change["add_variables"], ["fecha"])
        self.assertEqual(p_change["remove_variables"], ["dni"])

        # Verificar detalle de diffs
        self.assertEqual(len(plan["diffs"]), 1)
        diff = plan["diffs"][0]
        self.assertEqual(diff["diffs"]["model"]["old"], "gpt-4o-mini")
        self.assertEqual(diff["diffs"]["model"]["new"], "gpt-4o")
        self.assertEqual(diff["diffs"]["temperature"]["old"], "1.0")
        self.assertEqual(diff["diffs"]["temperature"]["new"], "0.2")

    @patch("Compare_ui.put_raw_item")
    @patch("Compare_ui.get_raw_item")
    @patch("Compare_ui.save_backup")
    def test_apply_batch_with_set_variables(self, mock_save_backup, mock_get_raw_item, mock_put_raw_item):
        """Verifica que apply_models_batch asigne exactamente la lista de variables cuando se envía set_variables."""
        raw_bot = {
            "client": {"S": "demo"},
            "bot": {"S": "chatbot"},
            "AGENT_CONFIG": {
                "M": {
                    "PROMPTS": {
                        "M": {
                            "SALUDO": {
                                "M": {
                                    "OPENAI_MODEL": {"S": "gpt-4o-mini"},
                                    "TEMPERATURE": {"N": "0.7"},
                                    "INPUT_VARIABLE": {"L": [{"S": "VAR_VIEJA_1"}, {"S": "VAR_VIEJA_2"}]}
                                }
                            }
                        }
                    }
                }
            }
        }

        mock_get_raw_item.return_value = json.loads(json.dumps(raw_bot))
        mock_save_backup.return_value = "backups/test_backup.json"

        changes = [
            {
                "client": "demo",
                "bot": "chatbot",
                "prompts": {
                    "SALUDO": {
                        "model": "gpt-4o",
                        "temperature": "0.3",
                        "set_variables": ["VAR_NUEVA_A", "VAR_NUEVA_B"]
                    }
                }
            }
        ]

        res = Compare_ui.apply_models_batch("desa", changes)
        self.assertTrue(res["ok"])
        self.assertEqual(res["bot_count"], 1)
        self.assertEqual(res["prompt_count"], 1)

        # Verificar el ítem escrito vía put_raw_item
        written_item = mock_put_raw_item.call_args[0][1]
        written_prompt = written_item["AGENT_CONFIG"]["M"]["PROMPTS"]["M"]["SALUDO"]["M"]
        self.assertEqual(written_prompt["OPENAI_MODEL"]["S"], "gpt-4o")
        self.assertEqual(written_prompt["TEMPERATURE"]["N"], "0.3")
        self.assertEqual(
            written_prompt["INPUT_VARIABLE"]["L"],
            [{"S": "VAR_NUEVA_A"}, {"S": "VAR_NUEVA_B"}]
        )


if __name__ == "__main__":
    unittest.main()
