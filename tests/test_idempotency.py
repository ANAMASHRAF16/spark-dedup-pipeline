"""
Test that re-running the ETL job does NOT create duplicates.

Usage:
    python tests/test_idempotency.py
"""

import os
import sys
import shutil

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from pyspark.sql import SparkSession
from generate_data import generate
from etl_job import run, GOLD_PATH


def test_idempotency():
    # Clean gold layer
    if os.path.exists(GOLD_PATH):
        shutil.rmtree(GOLD_PATH)

    # Generate fresh silver data
    generate()

    # Run ETL job FIRST time
    print("\n=== RUN 1 ===")
    run()

    # Count after first run
    spark = SparkSession.builder \
        .appName("Test") \
        .master("local[*]") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    count_after_run1 = spark.read.format("delta").load(GOLD_PATH).count()
    print(f"\nAfter run 1: {count_after_run1} records")

    spark.stop()

    # Run ETL job SECOND time (re-run)
    print("\n=== RUN 2 (re-run) ===")
    run()

    # Count after second run
    spark = SparkSession.builder \
        .appName("Test") \
        .master("local[*]") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    count_after_run2 = spark.read.format("delta").load(GOLD_PATH).count()
    print(f"\nAfter run 2: {count_after_run2} records")

    # Assert no duplicates
    if count_after_run1 == count_after_run2:
        print(f"\nPASSED: No duplicates. Count stayed at {count_after_run1}.")
    else:
        print(f"\nFAILED: Duplicates detected! Run 1: {count_after_run1}, Run 2: {count_after_run2}")
        sys.exit(1)

    spark.stop()


if __name__ == "__main__":
    test_idempotency()
