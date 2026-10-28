import unittest
import re
import Compare_ui


class TestStage3Editor(unittest.TestCase):

    def test_validation_logic_and_errors_in_js(self):
        """Verifica que el JS contenga el estado de validación y los mensajes de error requeridos."""
        html = Compare_ui.HTML

        # validationErrors state
        self.assertIn('validationErrors:', html)
        self.assertIn('temperature: null', html)
        self.assertIn('max_tokens: null', html)

        # Temperature validation error message
        self.assertIn('La temperatura va de 0 a 2. Usá punto para decimales (0.7).', html)

        # Tokens validation error message
        self.assertIn('Max tokens debe ser un entero mayor a 0.', html)

    def test_diff_preview_rendering_in_js(self):
        """Verifica que la función de renderizado de celdas genere el diff visual (del/ins)."""
        html = Compare_ui.HTML

        self.assertIn('function renderDiffCell(', html)
        self.assertIn('diff-cell-preview', html)
        self.assertIn('diff-old', html)
        self.assertIn('diff-arrow', html)
        self.assertIn('diff-new', html)

    def test_clear_draft_functionality(self):
        """Verifica que exista la función clearDraftEdits para resetear inputs y errores."""
        html = Compare_ui.HTML

        self.assertIn('function clearDraftEdits(){', html)
        self.assertIn('rightClearChanges', html)

    def test_safety_constraint_btn_review_disabled(self):
        """Verifica que btnReviewChanges arranque disabled en HTML y se controle por canReview en JS."""
        html = Compare_ui.HTML

        # Disabled attribute in HTML button tag
        match = re.search(r'<button[^>]*id="btnReviewChanges"[^>]*>', html)
        self.assertIsNotNone(match)
        self.assertIn('disabled', match.group(0))

        # Dynamic control in JS updateRightColumn()
        self.assertIn('btnReview.disabled = !canReview;', html)


if __name__ == '__main__':
    unittest.main()
