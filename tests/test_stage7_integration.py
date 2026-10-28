import unittest
from unittest.mock import MagicMock, patch
import json
import os
import tempfile
from io import BytesIO

import Compare_ui
from models_manager import compute_prompts_hash, extract_flat_rows, build_indices


class TestStage7Integration(unittest.TestCase):

    def test_other_views_integrity(self):
        """Verifica estrictamente que las otras vistas (JSON individual, Texto prompts, Comparar) no fueron alteradas."""
        html = Compare_ui.HTML

        # 1. View selector options
        self.assertIn('<option value="compare">Comparar</option>', html)
        self.assertIn('<option value="single">JSON individual</option>', html)
        self.assertNotIn('<option value="prompts">Texto (prompts)</option>', html)
        self.assertIn('<option value="models">Modelos</option>', html)
        self.assertIn('<option value="apikeys">API keys</option>', html)

        # 2. JSON individual view elements
        self.assertIn('id="singleBar"', html)
        self.assertIn('id="singleEditor"', html)
        self.assertIn('id="subModeJson"', html)
        self.assertIn('id="saveSingle"', html)

        # 3. Texto (prompts) view elements
        self.assertIn('id="promptsView"', html)
        self.assertIn('id="promptsSidebar"', html)
        self.assertIn('id="promptsMain"', html)
        self.assertIn('id="promptsRightJson"', html)
        self.assertIn('id="templateMonacoEditor"', html)

        # 4. Comparar view elements
        self.assertIn('id="paneBar"', html)
        self.assertIn('id="envL"', html)
        self.assertIn('id="envR"', html)
        self.assertIn('id="editor"', html)
        self.assertIn('id="saveL"', html)
        self.assertIn('id="saveR"', html)

        # 5. Excel buttons and Historial
        self.assertIn('id="modelBtnExcel"', html)
        self.assertIn('id="modelBtnHistory"', html)
        self.assertIn('id="historyModal"', html)

    @patch("Compare_ui.put_raw_item")
    @patch("Compare_ui.get_raw_item")
    @patch("Compare_ui.save_backup")
    def test_flow_bulk_migrate_and_rollback(self, mock_save_backup, mock_get_raw_item, mock_put_raw_item):
        """Prueba flujo completo: migración de modelo en múltiples bots, guardado de backup y rollback."""
        bot_a = {
            "client": {"S": "cliente1"},
            "bot": {"S": "bota"},
            "AGENT_CONFIG": {
                "M": {
                    "PROMPTS": {
                        "M": {
                            "PROMPT_1": {
                                "M": {
                                    "OPENAI_MODEL": {"S": "gpt-4o"},
                                    "TEMPERATURE": {"N": "0.5"}
                                }
                            }
                        }
                    }
                }
            }
        }
        bot_b = {
            "client": {"S": "cliente2"},
            "bot": {"S": "botb"},
            "AGENT_CONFIG": {
                "M": {
                    "PROMPTS": {
                        "M": {
                            "PROMPT_2": {
                                "M": {
                                    "OPENAI_MODEL": {"S": "gpt-4o"},
                                    "TEMPERATURE": {"N": "0.7"}
                                }
                            }
                        }
                    }
                }
            }
        }

        mock_get_raw_item.side_effect = lambda env, key: (
            json.loads(json.dumps(bot_a)) if key == ("cliente1", "bota")
            else json.loads(json.dumps(bot_b)) if key == ("cliente2", "botb")
            else None
        )
        mock_save_backup.side_effect = lambda env, c, b, raw: f"backups/{c}_{b}.json"

        changes = [
            {
                "client": "cliente1",
                "bot": "bota",
                "prompts": {"PROMPT_1": {"model": "gpt-4o-mini"}}
            },
            {
                "client": "cliente2",
                "bot": "botb",
                "prompts": {"PROMPT_2": {"model": "gpt-4o-mini"}}
            }
        ]

        # 1. Apply in TEST
        res = Compare_ui.apply_models_batch("test", changes, confirm_prod=False)
        self.assertTrue(res["ok"])
        self.assertEqual(res["bot_count"], 2)
        self.assertEqual(res["prompt_count"], 2)
        batch_id = res["batch_id"]

        # Check DynamoDB puts
        self.assertEqual(mock_put_raw_item.call_count, 2)
        item1 = mock_put_raw_item.call_args_list[0][0][1]
        item2 = mock_put_raw_item.call_args_list[1][0][1]
        self.assertEqual(item1["AGENT_CONFIG"]["M"]["PROMPTS"]["M"]["PROMPT_1"]["M"]["OPENAI_MODEL"]["S"], "gpt-4o-mini")
        self.assertEqual(item2["AGENT_CONFIG"]["M"]["PROMPTS"]["M"]["PROMPT_2"]["M"]["OPENAI_MODEL"]["S"], "gpt-4o-mini")

        # 2. Rollback
        with tempfile.TemporaryDirectory() as tmpdir:
            f1 = os.path.join(tmpdir, "cliente1_bota.json")
            f2 = os.path.join(tmpdir, "cliente2_botb.json")
            with open(f1, "w", encoding="utf-8") as f:
                json.dump(bot_a, f)
            with open(f2, "w", encoding="utf-8") as f:
                json.dump(bot_b, f)

            base_dir = os.path.dirname(os.path.abspath(Compare_ui.__file__))
            Compare_ui._undo_batches[batch_id]["backups"] = [
                {"client": "cliente1", "bot": "bota", "backup_file": os.path.relpath(f1, base_dir)},
                {"client": "cliente2", "bot": "botb", "backup_file": os.path.relpath(f2, base_dir)},
            ]

            undo_res = Compare_ui.undo_models_batch(batch_id)
            self.assertTrue(undo_res["ok"])
            self.assertEqual(undo_res["restored_bots"], 2)
            self.assertEqual(mock_put_raw_item.call_count, 4)
            restored_item1 = mock_put_raw_item.call_args_list[2][0][1]
            self.assertEqual(restored_item1["AGENT_CONFIG"]["M"]["PROMPTS"]["M"]["PROMPT_1"]["M"]["OPENAI_MODEL"]["S"], "gpt-4o")

    def test_excel_export_both_modes(self):
        """Verifica que el generador de Excel funcione correctamente para modelos."""
        selected_models = [
            {
                "model": "gpt-4o",
                "items": [
                    {
                        "client": "demo_client",
                        "bot": "demo_bot",
                        "prompt_details": [
                            {
                                "name": "SALUDO",
                                "path": ["AGENT_CONFIG", "PROMPTS", "SALUDO"],
                                "model": "gpt-4o",
                                "temperature": "0.7",
                                "reasoning_effort": "medium",
                                "response_format_type": "text",
                                "max_completion_tokens": "1000",
                            }
                        ]
                    }
                ]
            }
        ]
        wb = Compare_ui.model_workbook("desa", selected_models)
        self.assertIsNotNone(wb)
        out = BytesIO()
        wb.save(out)
        self.assertGreater(len(out.getvalue()), 1000)


if __name__ == '__main__':
    unittest.main()
