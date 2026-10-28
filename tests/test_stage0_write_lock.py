"""Etapa 6 y 7: la vista Modelos escribe de forma segura con UpdateItem y confirmación en PROD."""
import json
import threading
import unittest
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer
from unittest.mock import patch

import Compare_ui


class TestStage6WriteSecurity(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), Compare_ui.Handler)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()

    def post(self, path, payload):
        req = urllib.request.Request(
            f"http://127.0.0.1:{self.port}{path}",
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                return resp.status, json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read().decode("utf-8"))

    def test_flag_is_on(self):
        self.assertTrue(Compare_ui.MODELS_WRITE_ENABLED)

    def test_prod_without_confirm_is_blocked(self):
        payload = {
            "env": "prod",
            "changes": [{"client": "c", "bot": "b", "prompts": {"P": {"model": "x"}}}],
            "confirm_prod": False
        }
        with patch("Compare_ui.get_raw_item") as g, patch("Compare_ui.put_raw_item") as p:
            status, body = self.post("/api/models/apply-batch", payload)
            self.assertIn(status, (400, 403, 500))
            self.assertIn("error", body)
            p.assert_not_called()

    def test_frontend_apply_and_undo_enabled(self):
        html = Compare_ui.HTML
        self.assertIn("const MODELS_APPLY_ENABLED = true;", html)
        self.assertIn("async function applyModelChangesBatch(){\n  if(!MODELS_APPLY_ENABLED) return;", html)
        self.assertIn("async function undoModelChangesBatch(){\n  if(!MODELS_APPLY_ENABLED) return;", html)
        self.assertIn("btnConfirm.disabled = isProd;", html)
        self.assertIn("const isConfirmed = e.target.value.trim() === 'PROD';", html)


if __name__ == "__main__":
    unittest.main()
