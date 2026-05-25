from __future__ import annotations

import pytest

from swiss_exp_tracker.pipeline_ingestion.data_models.grocery import normalize_article


@pytest.mark.parametrize(
    "article, expected",
    [
        # Weight/volume suffix stripped
        ("Haribo Goldbären 100g", "Haribo Goldbären"),
        ("Aproz Gazeifiee 50cl", "Aproz Gazeifiee"),
        ("Vollmilch Past 1l", "Vollmilch Past"),
        ("Migros Wasser 1.5l", "Migros Wasser"),
        ("Mehl 1kg", "Mehl"),
        ("Salz 500mg", "Salz"),
        ("Sirup 250ml", "Sirup"),
        # Count x size pattern stripped
        ("Eier Freiland 6*53g", "Eier Freiland"),
        ("Riegel 4x100g", "Riegel"),
        ("Yogurt 2x500ml", "Yogurt"),
        # Trailing integer stripped
        ("Brot 750", "Brot"),
        # No suffix: preserved as-is
        ("Erdbeeren", "Erdbeeren"),
        ("Migros Caffé Macchiato", "Migros Caffé Macchiato"),
        # "7UP" preserved: leading digit but no unit suffix and no trailing-integer pattern
        ("7UP", "7UP"),
        # Extra whitespace collapsed
        ("Migros  Spaghetti  500g", "Migros Spaghetti"),
        # Mixed case unit suffix stripped
        ("Drink 330ML", "Drink"),
    ],
)
def test_normalize_article(article: str, expected: str) -> None:
    assert normalize_article(article) == expected
