#!/usr/bin/env python3
"""
Embed and upsert recipe chunks into Qdrant Cloud with sharding, checkpointing, and MLflow logging.

Usage examples (from Lightning terminal):

python -m src.index.embed_upsert --start 0 --limit 200000 --batch-size 256 --checkpoint ckpt_0_200k.json
python -m src.index.embed_upsert --start 200000 --limit 200000 --batch-size 256 --checkpoint ckpt_200k_400k.json
"""

import os
import argparse
import json
import time
from pathlib import Path
from typing import Optional
import torch

import pandas as pd
import numpy as np
from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient
from qdrant_client.models import VectorParams, Distance, PointStruct

import mlflow

# --------------------------------------------------------
# ✅ Utility functions
# --------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(description="Embed and upsert chunks to Qdrant Cloud")
    parser.add_argument("--chunks", type=str, default="data/processed/chunks.parquet",
                        help="Path to processed chunks parquet")
    parser.add_argument("--collection", type=str, default="recipes_chunks",
                        help="Qdrant collection name")
    parser.add_argument("--start", type=int, default=0,
                        help="Row index to start embedding from")
    parser.add_argument("--limit", type=int, default=0,
                        help="Number of rows to embed (0 = all remaining)")
    parser.add_argument("--batch-size", type=int, default=256,
                        help="Mini-batch size for encoding and upserting")
    parser.add_argument("--checkpoint", type=str, default="embed_ckpt.json",
                        help="Checkpoint file to resume from")
    return parser.parse_args()

def load_checkpoint(path: str) -> int:
    if not os.path.exists(path):
        return 0
    try:
        with open(path, "r") as f:
            data = json.load(f)
            return int(data.get("last_index", 0))
    except Exception:
        return 0

def save_checkpoint(path: str, idx: int) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump({"last_index": idx, "ts": time.time()}, f)
    os.replace(tmp, path)

def make_qdrant_client() -> QdrantClient:
    url = os.environ["QDRANT_URL"]
    api_key = os.environ.get("QDRANT_API_KEY")
    return QdrantClient(url=url, api_key=api_key, prefer_grpc=False)

# --------------------------------------------------------
# ✅ Main embedding + upsert logic
# --------------------------------------------------------

def main() -> None:
    args = parse_args()

    # Load environment variables
    model_name = os.environ.get("EMBEDDING_MODEL", "all-MiniLM-L6-v2")
    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI", "file:///workspace/mlruns")

    # Init MLflow
    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment("embedding-job")

    df = pd.read_parquet(args.chunks)
    total_rows = len(df)

    # Compute slice range
    start = max(args.start, load_checkpoint(args.checkpoint))
    end = total_rows if args.limit == 0 else min(total_rows, start + args.limit)
    df = df.iloc[start:end].reset_index(drop=True)
    print(f"Processing rows {start} → {end} (total: {total_rows})")

    # Prepare model & client
    use_cuda = os.environ.get("USE_CUDA", "0") == "1"
    device = "cuda" if (use_cuda and torch.cuda.is_available()) else "cpu"
    print(f"[embed] USE_CUDA={use_cuda} torch.cuda.is_available()={torch.cuda.is_available()} -> device={device}")
    model = SentenceTransformer(model_name, device=device)
    client = make_qdrant_client()

    # Create collection if it doesn't exist
    dim = model.get_sentence_embedding_dimension()
    assert dim is not None, "Embedding dimension could not be detected"
    collections = [c.name for c in client.get_collections().collections]
    if args.collection not in collections:
        print(f"Creating collection '{args.collection}' (dim={dim})...")
        client.create_collection(
            collection_name=args.collection,
            vectors_config=VectorParams(size=dim, distance=Distance.COSINE)
        )

    # ----------------------------------------------------
    # ✅ Embedding loop
    # ----------------------------------------------------
    with mlflow.start_run(run_name=f"embed_{start}_{end}") as run:
        mlflow.log_param("model", model_name)
        mlflow.log_param("collection", args.collection)
        mlflow.log_param("batch_size", args.batch_size)
        mlflow.log_param("start", start)
        mlflow.log_param("end", end)

        for i in range(0, len(df), args.batch_size):
            j = min(i + args.batch_size, len(df))
            batch = df.iloc[i:j]

            texts = batch["text"].astype(str).tolist()
            embeddings = model.encode(
                texts,
                batch_size=args.batch_size,
                show_progress_bar=False,
                convert_to_numpy=True
            )

            points = []
            for row_idx, emb in enumerate(embeddings):
                row = batch.iloc[row_idx]
                payload = row.to_dict()
                pid = int(row["doc_id"]) * 10_000 + int(row["chunk_id"])
                points.append(
                    PointStruct(id=pid, vector=emb.tolist(), payload=payload)
                )

            client.upsert(collection_name=args.collection, points=points, wait=True)
            save_checkpoint(args.checkpoint, start + j)

            progress = start + j
            pct = 100 * progress / total_rows
            print(f" Upserted {progress}/{total_rows} ({pct:.2f}%)")
            mlflow.log_metric("progress", progress, step=progress)

        print(" Embedding + upsert complete!")
        mlflow.log_metric("total_points", len(df))

if __name__ == "__main__":
    main()
