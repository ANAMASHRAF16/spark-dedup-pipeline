# Spark Dedup Pipeline

PySpark ETL job that reads documents from a silver layer, chunks them, and writes to a gold layer.

## Architecture

```
Silver (parquet) → Spark Job (chunk docs) → Gold (parquet)
```

## Run

```bash
pip install -r requirements.txt
python src/generate_data.py
python src/etl_job.py
```

## Known Issues

- Uses `mode("append")` — re-running the job creates duplicate records
- `monotonically_increasing_id()` gives unstable chunk IDs across runs
- No upsert logic — pipeline is not idempotent
