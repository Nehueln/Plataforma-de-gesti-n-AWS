import unittest
import re
import Compare_ui


class TestStage5ByModel(unittest.TestCase):

    def test_model_mode_table_structure(self):
        """Verifica que en el modo Por modelo la tabla incluya encabezados para Prompt y Modelo."""
        html = Compare_ui.HTML
        self.assertIn('<th class="th-prompt">Prompt</th>', html)
        self.assertIn('<th class="th-model">Modelo</th>', html)
        self.assertIn('middleCols = promptCell + modelCell;', html)

    def test_model_mode_title_diff_preview(self):
        """Verifica que el título del encabezado central muestre la migración old → new en modo Por modelo."""
        html = Compare_ui.HTML
        self.assertIn('titleEl.innerHTML = `<span class="diff-old">${esc(modelsState.selectedItem)}</span> <span class="diff-arrow">→</span> <span class="diff-new">${esc(modelsState.draft.model)}</span>`;', html)

    def test_model_mode_editor_label_and_summary(self):
        """Verifica que el label del selector de modelo y el resumen del pie se adapten a la migración masiva."""
        html = Compare_ui.HTML
        self.assertIn('Nuevo modelo (migrar de', html)
        self.assertIn('Migrar ${modelsState.selectedItem} → ${modelsState.draft.model}', html)

    def test_safety_btn_review_still_disabled(self):
        """Verifica que btnReviewChanges arranque disabled en HTML y se controle por canReview en JS."""
        html = Compare_ui.HTML
        match = re.search(r'<button[^>]*id="btnReviewChanges"[^>]*>', html)
        self.assertIsNotNone(match)
        self.assertIn('disabled', match.group(0))
        self.assertIn('btnReview.disabled = !canReview;', html)


if __name__ == '__main__':
    unittest.main()
