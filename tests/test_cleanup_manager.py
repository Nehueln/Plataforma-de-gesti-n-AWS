import unittest
from unittest.mock import MagicMock, patch
import json
import os
import tempfile

import cleanup_manager


class TestCleanupManager(unittest.TestCase):

    def test_get_bot_system_id(self):
        # Case 1: with STEP_FUNCTION_ARN
        item_with_sm = {
            "BUILDERBOT_CONFIG": {
                "M": {
                    "EXECUTION": {
                        "M": {
                            "STEP_FUNCTION_ARN": {
                                "S": "arn:aws:states:us-east-1:123456:stateMachine:desa_bera_ventas_chatbot_processor"
                            }
                        }
                    }
                }
            }
        }
        self.assertEqual(
            cleanup_manager.get_bot_system_id(item_with_sm, "ventas"),
            "desa_bera_ventas_chatbot_processor"
        )

        # Case 2: with BOT_ROLE_ID
        item_with_role = {
            "AGENT_CONFIG": {
                "M": {
                    "BOT_ROLE_ID": {"S": "3"}
                }
            }
        }
        self.assertEqual(
            cleanup_manager.get_bot_system_id(item_with_role, "cobranza"),
            "agente-cobranza (rol 3)"
        )

        # Case 3: None or empty
        self.assertEqual(cleanup_manager.get_bot_system_id(None, "deuda"), "agente-deuda")

    @patch("cleanup_manager.table_for")
    def test_scan_single_client_records(self, mock_table_for):
        mock_t = MagicMock()
        mock_t.name = "desa_sonia_config"
        mock_table_for.return_value = mock_t

        stats = cleanup_manager.scan_single_client_records("desa", "bera", ["ventas", "tramites", "cobranza"])
        
        # Cada bot representa 1 registro (su existencia y configuración en DynamoDB)
        self.assertEqual(stats["ventas"]["records_count"], 1)
        self.assertEqual(stats["tramites"]["records_count"], 1)
        self.assertEqual(stats["cobranza"]["records_count"], 1)
        self.assertIn("desa_sonia_config", stats["ventas"]["tables"])

    @patch("cleanup_manager.table_for")
    @patch("cleanup_manager.scan_keys")
    def test_get_cleanup_overview(self, mock_scan_keys, mock_table_for):
        mock_scan_keys.return_value = [
            ("bera", "ventas"),
            ("bera", "tramites"),
            ("corrientes", "canales_web")
        ]
        mock_t = MagicMock()
        mock_t.name = "desa_sonia_config"
        mock_table_for.return_value = mock_t

        overview = cleanup_manager.get_cleanup_overview("desa", refresh=True, load_records=True)
        self.assertEqual(overview["env"], "desa")
        self.assertEqual(overview["total_clients"], 2)
        self.assertEqual(overview["total_bots"], 3)
        self.assertEqual(overview["total_records"], 3)
        self.assertEqual(len(overview["clients"]), 2)
        self.assertEqual(overview["clients"][0]["client"], "bera")
        self.assertEqual(overview["clients"][0]["bots_count"], 2)
        self.assertTrue(overview["clients"][0]["bots"][0]["has_records"])

    @patch("cleanup_manager.client_for")
    @patch("cleanup_manager.table_for")
    @patch("cleanup_manager.scan_keys")
    def test_delete_agent_records_prod_security(self, mock_scan_keys, mock_table_for, mock_client_for):
        mock_scan_keys.return_value = [("bera", "ventas")]

        # Case 1: prod without confirmation must raise PermissionError
        with self.assertRaises(PermissionError):
            cleanup_manager.delete_agent_records("prod", [{"client": "bera", "bot": "ventas"}], confirm_prod=False)

        # Case 2: invalid bot must raise ValueError
        with self.assertRaises(ValueError):
            cleanup_manager.delete_agent_records("desa", [{"client": "bera", "bot": "nonexistent"}], confirm_prod=False)

    @patch("cleanup_manager.get_item")
    @patch("cleanup_manager.client_for")
    @patch("cleanup_manager.table_for")
    @patch("cleanup_manager.scan_keys")
    def test_delete_agent_records_success(self, mock_scan_keys, mock_table_for, mock_client_for, mock_get_item):
        mock_scan_keys.return_value = [("bera", "ventas")]
        mock_t = MagicMock()
        mock_t.name = "desa_sonia_config"
        mock_table_for.return_value = mock_t
        mock_c = MagicMock()
        mock_client_for.return_value = mock_c
        mock_get_item.return_value = {"client": "bera", "bot": "ventas", "AGENT_CONFIG": {}}

        res = cleanup_manager.delete_agent_records(
            "desa",
            [{"client": "bera", "bot": "ventas"}],
            confirm_prod=False,
            operator="test_user"
        )

        self.assertEqual(res["status"], "success")
        self.assertEqual(res["total_deleted"], 1)
        self.assertEqual(len(res["success_bots"]), 1)
        self.assertEqual(res["success_bots"][0]["bot"], "ventas")
        self.assertEqual(len(res["backup_files"]), 1)
        # Verifica que se haya llamado delete_item sobre la tabla de DynamoDB
        mock_t.delete_item.assert_called_once_with(Key={"client": "bera", "bot": "ventas"})

    @patch("cleanup_manager.get_cleanup_overview")
    def test_export_selection_csv(self, mock_overview):
        mock_overview.return_value = {
            "clients": [
                {
                    "client": "bera",
                    "bots": [
                        {
                            "bot": "ventas",
                            "system_id": "agente-ventas",
                            "records_count": 1,
                            "tables": {"desa_sonia_config": 1},
                            "last_updated": "Configurado en DynamoDB"
                        }
                    ]
                }
            ]
        }

        csv_text = cleanup_manager.export_selection_csv("desa", [{"client": "bera", "bot": "ventas"}])
        # Check UTF-8 BOM
        self.assertTrue(csv_text.startswith("\ufeff"))
        self.assertIn("Ambiente,Cliente,Bot / Agente", csv_text)
        self.assertIn("DESA,bera,ventas,agente-ventas,1", csv_text)

    def test_ui_html_cleanup_integration(self):
        """Verifica que Compare_ui.HTML contenga todos los controles, paneles, modales y scripts de Limpieza."""
        import Compare_ui
        html = Compare_ui.HTML

        # Selector de vista
        self.assertIn('<option value="cleanup">Limpieza DynamoDB</option>', html)

        # Controles superiores
        self.assertIn('id="cleanupTopControls"', html)
        self.assertIn('id="cleanupEnvSegmented"', html)
        self.assertIn('id="cleanupEnvSummary"', html)
        self.assertIn('id="cleanupGlobalSearch"', html)
        self.assertIn('id="cleanupBtnRefresh"', html)

        # Panel principal y columnas
        self.assertIn('id="cleanupPanel"', html)
        self.assertIn('id="cleanupLeftCol"', html)
        self.assertIn('id="cleanupClientsList"', html)
        self.assertIn('id="cleanupCenterCol"', html)
        self.assertIn('id="cleanupSelectAllEnv"', html)
        self.assertIn('id="cleanupAccordionContainer"', html)
        self.assertIn('id="cleanupRightCol"', html)

        # Panel derecho y acciones
        self.assertIn('id="cleanupSelectedBotsCount"', html)
        self.assertIn('id="cleanupSelectedRecordsCount"', html)
        self.assertIn('id="btnDeleteSelectedRecords"', html)
        self.assertIn('id="cleanupConfirmCheckbox"', html)
        self.assertIn('id="btnExportCleanup"', html)

        # Modales
        self.assertIn('id="cleanupDeleteConfirmModal"', html)
        self.assertIn('id="cleanupModalProdWarning"', html)
        self.assertIn('id="cleanupModalProdInput"', html)
        self.assertIn('id="btnCleanupModalExecute"', html)
        self.assertIn('id="cleanupBotDetailModal"', html)
        self.assertIn('id="btnCleanupDetailDeleteOne"', html)

        # Funciones JavaScript clave
        self.assertIn('function initCleanupView()', html)
        self.assertIn('function setCleanupEnv(', html)
        self.assertIn('function loadCleanupData(', html)
        self.assertIn('function renderCleanupAccordion()', html)
        self.assertIn('function updateCleanupRightPanel()', html)
        self.assertIn('function executeCleanupDelete()', html)
        self.assertIn('function exportCleanupSelection()', html)
        self.assertIn('function openCleanupBotDetail(', html)


if __name__ == "__main__":
    unittest.main()
