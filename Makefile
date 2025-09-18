.PHONY: fmt lint type test ingest clean mlflow

fmt:
	ruff check --fix .
	black .

lint:
	ruff check .
	black --check .

type:
	mypy src

test:
	pytest -q

mlflow:
	mlflow ui --host 0.0.0.0 --port 5000 --backend-store-uri sqlite:////mlflow/mlflow.db --default-artifact-root /mlruns

ingest:
	dvc repro ingest

clean:
	rm -rf data/processed data/interim
