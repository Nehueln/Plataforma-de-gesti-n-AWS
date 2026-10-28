import unittest
from unittest.mock import MagicMock, patch
import json
import io
import re

import Compare_ui


class TestStage2UI(unittest.TestCase):

    def test_html_contains_stage2_dom_elements(self):
        """Verifica que el HTML renderizado tenga todos los elementos del rediseño de 3 columnas."""
        html = Compare_ui.HTML

        # Top bar
        self.assertIn('id="modelEnvSegmented"', html)
        self.assertIn('data-env="desa"', html)
        self.assertIn('data-env="test"', html)
        self.assertIn('data-env="prod"', html)
        self.assertIn('id="modelTopActions"', html)
        self.assertIn('id="modelBtnHistory"', html)
        self.assertIn('id="modelBtnExcel"', html)

        # Status bar
        self.assertIn('id="modelStatusBar"', html)
        self.assertIn('id="modelStatusText"', html)
        self.assertIn('id="modelStatusProdWarning"', html)

        # Left column
        self.assertIn('id="modelLeftCol"', html)
        self.assertIn('id="scopeByPrompt"', html)
        self.assertIn('id="scopeByModel"', html)
        self.assertIn('id="modelListTitle"', html)
        self.assertIn('id="modelListSearch"', html)
        self.assertIn('id="modelItemList"', html)

        # Center column
        self.assertIn('id="modelCenterCol"', html)
        self.assertIn('id="centerTargetTitle"', html)
        self.assertIn('id="centerTargetCounts"', html)
        self.assertIn('id="centerExplText"', html)
        self.assertIn('id="centerBotFilter"', html)
        self.assertIn('id="familyChips"', html)
        self.assertIn('id="groupChips"', html)
        self.assertIn('id="modelNewTable"', html)
        self.assertIn('id="selectAllVisible"', html)
        self.assertIn('id="modelTableBody"', html)

        # Right column
        self.assertIn('id="modelRightCol"', html)
        self.assertIn('id="rightSelectedCount"', html)
        self.assertIn('id="rightSelectedLabel"', html)
        self.assertIn('id="rightClearChanges"', html)
        self.assertIn('id="editModelSelect"', html)
        self.assertIn('id="editTempInput"', html)
        self.assertIn('id="editTokensInput"', html)
        self.assertIn('id="editReasoningSelect"', html)
        self.assertIn('id="editFormatSelect"', html)
        self.assertIn('id="variablesList"', html)
        self.assertIn('id="btnAddVar"', html)
        self.assertIn('id="btnReviewChanges"', html)

        # CRITICAL CONSTRAINT: btnReviewChanges MUST be disabled in HTML
        match = re.search(r'<button[^>]*id="btnReviewChanges"[^>]*>', html)
        self.assertIsNotNone(match, "btnReviewChanges no encontrado en HTML")
        self.assertIn('disabled', match.group(0), "btnReviewChanges DEBE estar deshabilitado hasta la Etapa 6")

    def test_javascript_state_and_functions_present(self):
        """Verifica que el JS del cliente incluya el state y las funciones del nuevo layout."""
        html = Compare_ui.HTML

        self.assertIn('const modelsState =', html)
        self.assertIn('loadModelsData', html)
        self.assertIn('renderLeftList', html)
        self.assertIn('renderCenterColumn', html)
        self.assertIn('updateRightColumn', html)
        self.assertIn('setModelsScopeMode', html)
        self.assertIn('selectScopeItem', html)
        self.assertIn('setModelEnvironment', html)
        self.assertIn('initModelsView', html)
        self.assertIn('downloadModelsExcel', html)

    def test_models_review_disabled_in_js(self):
        """Verifica que en el JS updateRightColumn() controle el botón de aplicar según cambios y errores."""
        html = Compare_ui.HTML
        self.assertIn('btnReview.disabled = !canReview;', html)


if __name__ == '__main__':
    unittest.main()
