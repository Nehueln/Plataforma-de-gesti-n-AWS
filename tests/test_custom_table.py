import unittest
from unittest.mock import MagicMock, patch
import io
import pandas as pd
from botocore.exceptions import ClientError

import custom_table_manager


class TestCustomTableManager(unittest.TestCase):

    def setUp(self):
        # Sample dataframe
        self.sample_data = {
            "custom_key": ["K1", "K2", "", "K4", "K2"],  # contains 1 empty, 1 duplicate (K2)
            "alias_display": ["Alias 1", "Alias 2", "Alias 3", "Alias 4", "Alias 5"],
            "cliente": ["Cli A", "Cli B", "Cli C", "Cli D", "Cli E"],
            "dni": [1001, 1002, 1003, 1004, 1005],
            "tipo_contacto": ["Tel", "Email", "Tel", "Email", "WhatsApp"],
            "deuda": ["1500.50", "2000", "0", "500", "300"],
            "extra_col": ["ignored1", "ignored2", "ignored3", "ignored4", "ignored5"]
        }
        self.df = pd.DataFrame(self.sample_data)
        self.csv_bytes = self.df.to_csv(index=False).encode('utf-8')

    def test_inspect_file_preview_all_matched(self):
        res = custom_table_manager.inspect_file_preview(
            self.csv_bytes,
            configured_columns=["custom_key", "alias_display", "cliente", "dni", "tipo_contacto", "deuda"],
            filename="test.csv"
        )
        self.assertEqual(res["total_rows"], 5)
        self.assertTrue(res["has_custom_key"])
        self.assertEqual(res["empty_keys"], 1)
        self.assertEqual(res["duplicate_keys"], 1)  # K2 duplicated
        self.assertEqual(len(res["missing_columns"]), 0)
        self.assertEqual(len(res["matched_columns"]), 6)
        self.assertEqual(len(res["preview_rows"]), 5)
        self.assertEqual(res["preview_rows"][0]["custom_key"], "K1")
        self.assertEqual(res["preview_rows"][0]["dni"], "1001")

    def test_inspect_file_preview_missing_columns(self):
        res = custom_table_manager.inspect_file_preview(
            self.csv_bytes,
            configured_columns=["custom_key", "alias_display", "inexistente_col"],
            filename="test.csv"
        )
        self.assertIn("inexistente_col", res["missing_columns"])
        self.assertEqual(len(res["missing_columns"]), 1)

    def test_process_and_upload_missing_column_error(self):
        mock_table = MagicMock()
        with self.assertRaises(ValueError) as ctx:
            custom_table_manager.process_and_upload(
                mock_table,
                self.csv_bytes,
                required_columns=["custom_key", "col_que_no_existe"],
                filename="test.csv"
            )
        self.assertIn("Faltan las siguientes columnas", str(ctx.exception))

    def test_process_and_upload_missing_custom_key_error(self):
        mock_table = MagicMock()
        with self.assertRaises(ValueError) as ctx:
            custom_table_manager.process_and_upload(
                mock_table,
                self.csv_bytes,
                required_columns=["alias_display", "cliente"],
                filename="test.csv"
            )
        self.assertIn("custom_key", str(ctx.exception))

    def test_process_and_upload_success(self):
        mock_table = MagicMock()
        batch_mock = MagicMock()
        mock_table.batch_writer.return_value.__enter__.return_value = batch_mock

        cols = ["custom_key", "alias_display", "cliente", "dni"]
        res = custom_table_manager.process_and_upload(
            mock_table,
            self.csv_bytes,
            required_columns=cols,
            filename="test.csv"
        )

        self.assertEqual(res["status"], "success")
        self.assertEqual(res["total_rows"], 5)
        # 1 row has empty custom_key (""), so 4 rows should be uploaded
        self.assertEqual(res["uploaded_records"], 4)
        self.assertEqual(res["skipped_empty_keys"], 1)
        self.assertEqual(batch_mock.put_item.call_count, 4)

        # Ensure extra_col was NOT uploaded
        for call_args in batch_mock.put_item.call_args_list:
            item = call_args[1]["Item"]
            self.assertNotIn("extra_col", item)
            self.assertIn("custom_key", item)

    @patch("custom_table_manager.get_existing_table")
    def test_execute_create_and_upload_validations(self, mock_get_table):
        mock_table = MagicMock()
        mock_get_table.return_value = mock_table

        # Invalid table name
        with self.assertRaises(ValueError):
            custom_table_manager.execute_create_and_upload("ab", self.csv_bytes, filename="test.csv")

        # Duplicate columns
        with self.assertRaises(ValueError):
            custom_table_manager.execute_create_and_upload(
                "valid_table_name",
                self.csv_bytes,
                required_columns=["custom_key", "alias", "alias"],
                filename="test.csv"
            )

    @patch("custom_table_manager.get_dynamodb_resource")
    def test_get_existing_table_exception_when_not_exists(self, mock_resource):
        mock_dyn = MagicMock()
        mock_table = MagicMock()
        mock_dyn.Table.return_value = mock_table
        mock_resource.return_value = mock_dyn

        # Simulate ResourceNotFoundException
        err_response = {'Error': {'Code': 'ResourceNotFoundException', 'Message': 'Cannot find table'}}
        mock_table.load.side_effect = ClientError(err_response, 'DescribeTable')

        with self.assertRaises(ValueError) as ctx:
            custom_table_manager.get_existing_table("tabla_inexistente", env="desa")
        self.assertIn("no existe en el ambiente DESA", str(ctx.exception))
        self.assertIn("debe existir previamente en DynamoDB", str(ctx.exception))

    def test_default_columns(self):
        self.assertIn("custom_key", custom_table_manager.DEFAULT_COLUMNS)
        self.assertEqual(custom_table_manager.DEFAULT_COLUMNS[0], "custom_key")

    def test_inspect_file_preview_excel(self):
        # Create Excel in-memory
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            self.df.to_excel(writer, index=False)
        excel_bytes = output.getvalue()

        res = custom_table_manager.inspect_file_preview(
            excel_bytes,
            configured_columns=["custom_key", "cliente"],
            filename="test.xlsx"
        )
        self.assertEqual(res["total_rows"], 5)
        self.assertTrue(res["has_custom_key"])
        self.assertEqual(len(res["missing_columns"]), 0)
        self.assertEqual(len(res["preview_rows"]), 5)


if __name__ == "__main__":
    unittest.main()
