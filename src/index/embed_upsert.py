from __future__ import annotations
from typing import Any, List
from pathlib import Path
import os, math
os.environ.setdefault("CUDA_VISIBLE_DEVICES", "")
import pandas as pd
from sentence_transformers import SentenceTransformer, util
import torch
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct

COLLECTION = "recipes_chunks"

def get_env(name: str, default: str | None = None) -> str:
    v = os.environ.get(name, default)
    if v is None:
        raise RuntimeError(f"Missing env var: {name}")
    return v

def main() -> None:
    qdrant_url = get_env("QDRANT_URL", "http://localhost:6333")
    model_name = get_env("EMBEDDING_MODEL", "all-MiniLM-L6-v2")

    df = pd.read_parquet("data/processed/chunks.parquet")
    texts = df["text"].astype(str).tolist()

    device = "cuda" if torch.cuda.is_available() else "cpu"
    model = SentenceTransformer(model_name, device="cpu")
    embeddings = model.encode(
    texts, batch_size=64, show_progress_bar=True, convert_to_numpy=True, device=device
    )
    # pool = model.start_multi_process_pool()
    # try:
    #     # Larger batch_size helps on CPU; tune 256..512 per your RAM
    #     embeddings = model.encode_multi_process(
    #         texts,
    #         pool,
    #         batch_size=256,
    #         show_progress_bar=True
    #     )
    # finally:
    #     model.stop_multi_process_pool(pool)


    out_vec = Path("data/processed/chunk_vectors.parquet")
    pd.DataFrame({"doc_id": df["doc_id"], "chunk_id": df["chunk_id"]}).assign(
        vector=list(map(lambda v: v.tolist(), embeddings))
    ).to_parquet(out_vec, index=False)

    # upsert to Qdrant
    client = QdrantClient(url=qdrant_url, prefer_grpc=False)

    dim = embeddings.shape[1]
    
    collections = [c.name for c in client.get_collections().collections]
    if COLLECTION not in collections:
        client.create_collection(
            collection_name=COLLECTION,
            vectors_config=VectorParams(size=dim, distance=Distance.COSINE),
        )

    points = []
    for i in range(len(df)):
        pid = int(df["doc_id"].iloc[i]) * 10_000 + int(df["chunk_id"].iloc[i])
        meta = {
            "doc_id": int(df["doc_id"].iloc[i]),
            "chunk_id": int(df["chunk_id"].iloc[i]),
            "title": (df["title"].iloc[i] if "title" in df.columns else None),
            "description": (df["description"].iloc[i] if "description" in df.columns else None),
            "keywords": (df["keywords"].iloc[i] if "keywords" in df.columns else None),
            "total_time": (df["total_time"].iloc[i] if "total_time" in df.columns else None),
            "text": df["text"].iloc[i],
        }
        points.append(PointStruct(id=pid, vector=embeddings[i].tolist(), payload=meta))

    # batch upload
    client.upsert(collection_name=COLLECTION, points=points, wait=True)
    print(f"Upserted {len(points)} points into {COLLECTION}")

if __name__ == "__main__":
    main()
