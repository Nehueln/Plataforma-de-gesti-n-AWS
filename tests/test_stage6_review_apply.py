import unittest
from unittest.mock import MagicMock, patch
import json
import os
import tempfile
import re

import Compare_ui
from models_manager import compute_prompts_hash


class TestStage6ReviewApply(unittest.TestCase):

    def test_modal_and_ui_elements_present(self):
        """Verifica la presencia en el HTML del modal de revisión, confirmación PROD y botón deshacer."""
        html = Compare_ui.HTML

        self.assertIn('id="modelReviewModal"', html)
        self.assertIn('id="modalReviewEnvBadge"', html)
        self.assertIn('id="modalReviewCounts"', html)
        self.assertIn('id="modalReviewCloseBtn"', html)
        self.assertIn('id="modalReviewSummaryBox"', html)
        self.assertIn('id="modalReviewProdWarning"', html)
        self.assertIn('id="modalReviewProdInput"', html)
        self.assertIn('id="modalReviewTableBody"', html)
        self.assertIn('id="btnModalReviewCancel"', html)
        self.assertIn('id="btnModalReviewConfirm"', html)
        self.assertIn('id="modelUndoBtn"', html)

        # JS functions
        self.assertIn('openModelReviewModal', html)
        self.assertIn('closeModelReviewModal', html)
        self.assertIn('applyModelChangesBatch', html)
        self.assertIn('undoModelChangesBatch', html)
        self.assertIn('/api/models/apply-batch', html)
        self.assertIn('/api/models/undo-batch', html)

    def test_apply_models_batch_validates_env_and_prod(self):
        """Verifica que rechace ambientes inválidos y bloquee PROD si confirm_prod no es True."""
        with self.assertRaises(ValueError):
            Compare_ui.apply_models_batch("invalid_env", [{"client": "c", "bot": "b"}])

        with self.assertRaises(ValueError) as cm:
            Compare_ui.apply_models_batch("prod", [{"client": "c", "bot": "b"}], confirm_prod=False)
        self.assertIn("PRODUCCIÓN", str(cm.exception))

    @patch("Compare_ui.put_raw_item")
    @patch("Compare_ui.get_raw_item")
    @patch("Compare_ui.save_backup")
    def test_apply_and_undo_models_batch(self, mock_save_backup, mock_get_raw_item, mock_put_raw_item):
        """Verifica la ejecución de un lote de cambios con backup, bloqueo optimista y posterior undo."""
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
                                    "INPUT_VARIABLE": {"L": [{"S": "NOMBRE"}, {"S": "FECHA"}]}
                                }
                            }
                        }
                    }
                }
            }
        }
        prompts_hash = compute_prompts_hash(raw_bot["AGENT_CONFIG"]["M"]["PROMPTS"]["M"])

        mock_get_raw_item.return_value = json.loads(json.dumps(raw_bot))
        mock_save_backup.return_value = "backups/mock_backup.json"

        changes = [
            {
                "client": "demo",
                "bot": "chatbot",
                "expected_hash": prompts_hash,
                "prompts": {
                    "SALUDO": {
                        "model": "gpt-5-mini",
                        "temperature": "0.2",
                        "add_variables": ["EDAD"],
                        "remove_variables": ["FECHA"]
                    }
                }
            }
        ]

        result = Compare_ui.apply_models_batch("desa", changes, confirm_prod=False)
        self.assertTrue(result["ok"])
        self.assertEqual(result["bot_count"], 1)
        self.assertEqual(result["prompt_count"], 1)
        batch_id = result["batch_id"]
        self.assertTrue(batch_id.startswith("batch_desa_"))

        # Check DynamoDB put was called with updated prompt
        mock_put_raw_item.assert_called_once()
        put_item = mock_put_raw_item.call_args[0][1]
        p_dict = put_item["AGENT_CONFIG"]["M"]["PROMPTS"]["M"]["SALUDO"]["M"]
        self.assertEqual(p_dict["OPENAI_MODEL"]["S"], "gpt-5-mini")
        self.assertEqual(p_dict["TEMPERATURE"]["N"], "0.2")
        vars_list = [v["S"] for v in p_dict["INPUT_VARIABLE"]["L"]]
        self.assertIn("NOMBRE", vars_list)
        self.assertIn("EDAD", vars_list)
        self.assertNotIn("FECHA", vars_list)

        # Test Undo
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", delete=False, suffix=".json") as tmp:
            json.dump(raw_bot, tmp)
            tmp_path = tmp.name

        try:
            rel_path = os.path.relpath(tmp_path, os.path.dirname(os.path.abspath(Compare_ui.__file__)))
            Compare_ui._undo_batches[batch_id]["backups"] = [
                {"client": "demo", "bot": "chatbot", "backup_file": rel_path}
            ]

            undo_res = Compare_ui.undo_models_batch(batch_id)
            self.assertTrue(undo_res["ok"])
            self.assertEqual(undo_res["restored_bots"], 1)

            # Check that undo item was put back
            self.assertEqual(mock_put_raw_item.call_count, 2)
            restored_item = mock_put_raw_item.call_args[0][1]
            restored_p = restored_item["AGENT_CONFIG"]["M"]["PROMPTS"]["M"]["SALUDO"]["M"]
            self.assertEqual(restored_p["OPENAI_MODEL"]["S"], "gpt-4o-mini")
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    @patch("Compare_ui.get_raw_item")
    def test_apply_models_batch_optimistic_locking(self, mock_get_raw_item):
        """Verifica que si otro proceso modificó el bot (hash diferente), se aborta la operación."""
        raw_bot = {
            "client": {"S": "demo"},
            "bot": {"S": "chatbot"},
            "AGENT_CONFIG": {
                "M": {
                    "PROMPTS": {
                        "M": {
                            "SALUDO": {
                                "M": {
                                    "OPENAI_MODEL": {"S": "gpt-4o-mini"}
                                }
                            }
                        }
                    }
                }
            }
        }
        mock_get_raw_item.return_value = raw_bot

        changes = [
            {
                "client": "demo",
                "bot": "chatbot",
                "expected_hash": "outdated_hash_12345",
                "prompts": {"SALUDO": {"model": "gpt-5"}}
            }
        ]

        with self.assertRaises(RuntimeError) as cm:
            Compare_ui.apply_models_batch("desa", changes)
        self.assertIn("otro proceso", str(cm.exception))


if __name__ == '__main__':
    unittest.main()
