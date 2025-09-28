from __future__ import annotations
from pathlib import Path
from typing import Any, Iterable
import pandas as pd, yaml, math

def read_params() -> dict[str, Any]:
    try:
        with open("params.yaml", "r") as f:
            return yaml.safe_load(f) or {}
    except FileNotFoundError:
        return {}

def _nz(x: Any) -> str:
    if x is None:
        return ""
    if isinstance(x, float) and math.isnan(x):
        return ""
    s = x
    # Flatten lists/tuples from parquet (ingredients/steps often are lists)
    if isinstance(x, (list, tuple)):
        s = ", ".join(str(v).strip() for v in x if v is not None and str(v).strip())
    return str(s).strip()

def _format_steps(x: Any) -> str:
    """Number steps if it's a list; otherwise return cleaned string."""
    if isinstance(x, (list, tuple)):
        lines = [str(v).strip() for v in x if v is not None and str(v).strip()]
        if not lines:
            return ""
        return "\n".join(f"{i+1}. {line}" for i, line in enumerate(lines))
    return _nz(x)

def _make_doc_text(row: pd.Series) -> str:
    parts = []
    if "Name" in row and _nz(row["Name"]):
        parts.append(f"Title: {_nz(row['Name'])}")
    if "Description" in row and _nz(row["Description"]):
        parts.append(f"Description: {_nz(row['Description'])}")
    if "Keywords" in row and _nz(row["Keywords"]):
        parts.append(f"Keywords: {_nz(row['Keywords'])}")
    if "RecipeIngredientParts" in row and _nz(row["RecipeIngredientParts"]):
        parts.append(f"Ingredients: {_nz(row['RecipeIngredientParts'])}")
    if "RecipeInstructions" in row and _format_steps(row["RecipeInstructions"]):
        parts.append(f"Steps:\n{_format_steps(row['RecipeInstructions'])}")
    return "\n".join([p for p in parts if p])

def _chunk_text(s: str, max_chars: int = 1000, overlap: int = 100) -> list[str]:
    if not s:
        return []
    chunks = []
    start = 0
    n = len(s)
    while start < n:
        end = min(n, start + max_chars)
        chunks.append(s[start:end])
        if end == n:
            break
        start = max(0, end - overlap)
    return chunks

def main() -> None:
    params = read_params()
    max_chars = int(params.get("chunk", {}).get("max_chars", 1000))
    overlap = int(params.get("chunk", {}).get("overlap", 100))

    src_path = Path("data/processed/recipes_clean.parquet")
    if not src_path.exists():
        raise FileNotFoundError(f"Missing input: {src_path}")

    df = pd.read_parquet(src_path)
    if df.empty:
        print("Input dataframe is empty; nothing to chunk.")
        out_path = Path("data/processed/chunks.parquet")
        pd.DataFrame(columns=["doc_id","chunk_id","text","title"]).to_parquet(out_path, index=False)
        return

    rows = []
    id_col = "RecipeId" if "RecipeId" in df.columns else None

    made = 0
    for i, row in df.iterrows():
        doc_id = int(row[id_col]) if id_col and pd.notna(row[id_col]) else int(i)
        base = _make_doc_text(row)
        chunks = _chunk_text(base, max_chars=max_chars, overlap=overlap)
        for j, c in enumerate(chunks):
            rows.append({
                "doc_id": doc_id,
                "chunk_id": j,
                "text": c,
                "title": row.get("Name", None),
                "description": row.get("Description", None),
                "keywords": row.get("Keywords", None),
                "total_time": row.get("TotalTime", None),
            })
        made += len(chunks)

    out = pd.DataFrame(rows, columns=["doc_id","chunk_id","text","title","description","keywords","total_time"])
    out_dir = Path("data/processed")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "chunks.parquet"
    out.to_parquet(out_path, index=False)
    print(f"Read {len(df)} recipes; wrote {len(out)} chunks to {out_path} "
          f"(max_chars={max_chars}, overlap={overlap}).")

if __name__ == "__main__":
    main()
