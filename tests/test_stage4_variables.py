import unittest
import re
import Compare_ui


class TestStage4Variables(unittest.TestCase):

    def test_variables_css_classes(self):
        """Verifica que el CSS incluya los estilos requeridos para pills agregadas, removidas y parciales."""
        html = Compare_ui.HTML
        self.assertIn('.var-pill.var-pill-added', html)
        self.assertIn('.var-pill.var-pill-removed', html)
        self.assertIn('.var-pill.var-pill-partial', html)
        self.assertIn('.var-partial-badge', html)
        self.assertIn('.var-undo-btn', html)

    def test_variables_table_diff(self):
        """Verifica que en la tabla central se compute e inyecte varsDiffHtml con .diff-added y .diff-removed."""
        html = Compare_ui.HTML
        self.assertIn('varsDiffHtml', html)
        self.assertIn('modelsState.draft.variablesToAdd.filter', html)
        self.assertIn('modelsState.draft.variablesToRemove.filter', html)
        self.assertIn('<div class="cell-vars-diff">', html)

    def test_variables_right_column_rendering_and_handlers(self):
        """Verifica que la columna derecha renderice los botones de quitar/deshacer y la función handleAddNewVariable."""
        html = Compare_ui.HTML
        self.assertIn('data-del-var', html)
        self.assertIn('data-undo-var', html)
        self.assertIn('data-cancel-add-var', html)
        self.assertIn('handleAddNewVariable', html)
        self.assertIn('newVarInput.disabled = true;', html)
        self.assertIn('newVarInput.disabled = false;', html)

    def test_summary_includes_variables(self):
        """Verifica que el resumen del pie refleje las variables agregadas y removidas."""
        html = Compare_ui.HTML
        self.assertIn('+var:', html)
        self.assertIn('-var:', html)

    def test_safety_btn_review_still_disabled(self):
        """Verifica que btnReviewChanges arranque disabled en HTML y se controle por canReview en JS."""
        html = Compare_ui.HTML
        match = re.search(r'<button[^>]*id="btnReviewChanges"[^>]*>', html)
        self.assertIsNotNone(match)
        self.assertIn('disabled', match.group(0))
        self.assertIn('btnReview.disabled = !canReview;', html)


if __name__ == '__main__':
    unittest.main()
