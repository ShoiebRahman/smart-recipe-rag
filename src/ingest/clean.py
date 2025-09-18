from __future__ import annotations

import re
from pathlib import Path

import pandas as pd
import yaml


def read_params() -> dict:
    with open("params.yaml") as f:
        return yaml.safe_load(f)


def parse_time_to_minutes(value: str) -> int:
    if pd.isna(value):
        return None
    if isinstance(value, (int, float)):
        return int(value)

    # Example formats: 'PT30M', 'PT1H20M', 'PT2H'
    match = re.match(r"PT(?:(\d+)H)?(?:(\d+)M)?", str(value))
    if match:
        hours = int(match.group(1)) if match.group(1) else 0
        minutes = int(match.group(2)) if match.group(2) else 0
        return hours * 60 + minutes
    return None


def basic_clean(df: pd.DataFrame, params: dict) -> pd.DataFrame:
    df = df.copy()

    # Drop nulls for key columns
    for col in ["Name", "RecipeIngredientParts"]:
        if col in df.columns:
            df = df[df[col].notna()]

    # Deduplicate based on config keys (only if those columns exist)
    dedupe_keys: list[str] = params["clean"]["dedupe_by"]
    dedupe_keys = [k for k in dedupe_keys if k in df.columns]
    if dedupe_keys:
        df = df.drop_duplicates(subset=dedupe_keys)

    # Filter by total cooking time if available
    if "TotalTime" in df.columns:
        df["TotalTime"] = df["TotalTime"].apply(parse_time_to_minutes)
        df = df.dropna(subset=["TotalTime"])
        df = df[(df["TotalTime"] > 0) & (df["TotalTime"] <= params["clean"]["max_minutes"])]

    # Normalize whitespace in recipe names
    if "Name" in df.columns:
        df["Name"] = df["Name"].astype(str).str.replace(r"\s+", " ", regex=True).str.strip()

    return df


def main():
    params = read_params()
    interim = Path(params["data"]["interim_dir"]) / "recipes_interim.parquet"
    df = pd.read_parquet(interim)
    cleaned = basic_clean(df, params)

    processed_dir = Path(params["data"]["processed_dir"])
    processed_dir.mkdir(parents=True, exist_ok=True)

    out = processed_dir / "recipes_clean.parquet"
    cleaned.to_parquet(out, index=False)
    print(f"Wrote processed: {out}")


if __name__ == "__main__":
    main()
