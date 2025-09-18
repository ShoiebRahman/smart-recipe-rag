# import mlflow, os, pandas as pd

# def main():
#     tracking_uri = os.environ.get("MLFLOW_TRACKING_URI", "sqlite:///mlflow/mlflow.db")
#     mlflow.set_tracking_uri(tracking_uri)
#     mlflow.set_experiment("recipe-data")
#     with mlflow.start_run(run_name="week1-data-clean"):
#         df = pd.read_parquet("data/processed/recipes_clean.parquet")
#         mlflow.log_metric("n_rows_clean", len(df))
#         mlflow.log_artifact("params.yaml")
#         # store a small sample for eyeballing
#         sample = df.sample(min(200, len(df)))
#         sample.to_csv("data/processed/sample_preview.csv", index=False)
#         mlflow.log_artifact("data/processed/sample_preview.csv")

# if __name__ == "__main__":
#     main()

import os

import pandas as pd

import mlflow


def main():
    # Resolve absolute DB path
    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI", "sqlite:///mlflow/mlflow.db")
    mlflow.set_tracking_uri(tracking_uri)

    # mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment("recipe-data")

    with mlflow.start_run(run_name="week1-data-clean") as run:
        df = pd.read_parquet("data/processed/recipes_clean.parquet")
        mlflow.log_metric("n_rows_clean", len(df))
        mlflow.log_artifact("params.yaml")

        # Store a small sample for eyeballing
        sample = df.sample(min(200, len(df)))
        sample_path = "data/processed/sample_preview.csv"
        sample.to_csv(sample_path, index=False)
        mlflow.log_artifact(sample_path)

        # Print confirmation info
        print(" -- Run completed -- ")
        print(f"Run ID: {run.info.run_id}")
        print(f"Experiment ID: {run.info.experiment_id}")
        print(f"Tracking URI: {tracking_uri}")
        print("DB file location: mlflow/mlflow.db")


if __name__ == "__main__":
    main()
