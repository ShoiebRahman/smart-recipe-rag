.PHONY: fmt lint type test ingest clean mlflow

fmt:
\truff check --fix .
\tblack .

lint:
\truff check .
\tblack --check .

type:
\tmypy src

test:
\tpytest -q

mlflow:
\tmlflow ui --backend-store-uri sqlite:///mlflow.db --default-artifact-root ./mlruns

# DVC pipeline steps
ingest:
\tdvc repro ingest

clean:
\trm -rf data/processed data/interim
