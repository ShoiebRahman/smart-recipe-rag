from pathlib import Path

import pandas as pd


def test_clean_outputs_exist():
    assert Path("data/processed/recipes_clean.parquet").exists()


def test_clean_has_min_columns():
    df = pd.read_parquet("data/processed/recipes_clean.parquet")
    assert len(df) > 0
    assert all(c in df.columns for c in ["Name", "RecipeIngredientParts", "TotalTime"])
