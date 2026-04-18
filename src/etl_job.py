"""
ETL Job — Reads documents from silver layer, chunks them, writes to gold layer.

Known issue: Uses append mode. Re-running creates duplicate records.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, current_timestamp, monotonically_increasing_id, udf
from pyspark.sql.types import ArrayType, StringType

SILVER_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "silver")
GOLD_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "gold")

CHUNK_SIZE = 200  # max characters per chunk


def create_spark():
    return SparkSession.builder \
        .appName("DocChunkETL") \
        .master("local[*]") \
        .getOrCreate()


def chunk_text(text, chunk_size=CHUNK_SIZE):
    """Split a long text into smaller pieces."""
    words = text.split()
    chunks = []
    current_chunk = []
    current_length = 0

    for word in words:
        if current_length + len(word) + 1 > chunk_size and current_chunk:
            chunks.append(" ".join(current_chunk))
            current_chunk = [word]
            current_length = len(word)
        else:
            current_chunk.append(word)
            current_length += len(word) + 1

    if current_chunk:
        chunks.append(" ".join(current_chunk))

    return chunks


def run():
    spark = create_spark()

    # Read documents from silver layer
    silver_df = spark.read.parquet(SILVER_PATH)
    print(f"Read {silver_df.count()} documents from silver layer")

    # Split each document into chunks
    chunk_udf = udf(lambda text: chunk_text(text), ArrayType(StringType()))

    chunked_df = silver_df \
        .withColumn("chunks", chunk_udf(col("content"))) \
        .select(
            col("doc_id"),
            col("title"),
            col("category"),
            explode(col("chunks")).alias("chunk_text"),
        ) \
        .withColumn("chunk_id", monotonically_increasing_id()) \
        .withColumn("processed_at", current_timestamp()) \
        .select("doc_id", "chunk_id", "title", "category", "chunk_text", "processed_at")

    print(f"Created {chunked_df.count()} chunks")

    # Write to gold layer — APPEND MODE
    chunked_df.write.mode("append").parquet(GOLD_PATH)

    # Show count
    gold_df = spark.read.parquet(GOLD_PATH)
    print(f"Gold layer now has {gold_df.count()} total records")

    spark.stop()


if __name__ == "__main__":
    run()
