from __future__ import annotations

from pathlib import Path

import pandas as pd
import yaml


def read_params() -> dict:
    with open("params.yaml") as f:
        return yaml.safe_load(f)


def load_raw(raw_dir: str) -> pd.DataFrame:
    raw_path = Path(raw_dir) / "recipes.csv"
    if not raw_path.exists():
        raise FileNotFoundError(f"'recipes.csv' not found in {raw_dir}.")
    df = pd.read_csv(raw_path)
    return df


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [c.strip() for c in df.columns]
    return df


def main():
    params = read_params()
    raw = load_raw(params["data"]["raw_dir"])
    norm = normalize_columns(raw)

    interim = Path(params["data"]["interim_dir"])
    interim.mkdir(parents=True, exist_ok=True)

    out_path = interim / "recipes_interim.parquet"
    norm.to_parquet(out_path, index=False)
    print(f"Wrote interim: {out_path}")


if __name__ == "__main__":
    main()
