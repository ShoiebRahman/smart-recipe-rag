import pandas as pd, os
def test_chunks_exist() -> None:
    assert os.path.exists("data/processed/chunks.parquet")
    df = pd.read_parquet("data/processed/chunks.parquet")
    assert len(df) > 0
    assert {"doc_id","chunk_id","text"}.issubset(df.columns)
