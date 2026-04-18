# Spark Dedup Pipeline

PySpark ETL job that reads documents from a silver layer, chunks them, and writes to a gold layer using Delta Lake.

## Architecture

```
Silver (parquet) → Spark Job (chunk docs) → Gold (Delta table)
```

## Setup

```bash
pip install -r requirements.txt
```

## Run

```bash
# Generate sample data
python src/generate_data.py

# Run ETL job
python src/etl_job.py

# Test idempotency (runs job twice, asserts no duplicates)
python tests/test_idempotency.py
```

## Fix Applied

Replaced `mode("append")` with Delta Lake `MERGE` (upsert) on `(doc_id, chunk_id)`.

| Change | Before | After |
|---|---|---|
| Write mode | `mode("append")` parquet | Delta Lake `MERGE` |
| Duplicate handling | None — duplicates on every re-run | Upsert on `(doc_id, chunk_id)` |
| chunk_id | `monotonically_increasing_id()` (unstable) | `row_number()` window (stable per doc) |
| Storage format | Parquet (no merge support) | Delta (supports MERGE) |
| Re-run result | Row count doubles each time | Row count stays the same |
