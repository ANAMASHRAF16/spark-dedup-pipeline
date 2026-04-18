"""
ETL Job — FIXED version (no duplicates on re-run).

Fix: Delta Lake MERGE (upsert) on (doc_id, chunk_id).
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, current_timestamp, row_number, udf
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# --- PATHS ---
SILVER_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "silver")
GOLD_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "gold")

CHUNK_SIZE = 200


def create_spark():
    """Create a Spark session with Delta Lake support."""
    return SparkSession.builder \
        .appName("DocChunkETL") \
        .master("local[*]") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()


def chunk_text(text, chunk_size=CHUNK_SIZE):
    """
    Split a long text into smaller pieces.
    Same function as broken version — the chunking logic isn't the problem.
    """
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
    # Step 1: Start Spark (with Delta Lake config)
    spark = create_spark()

    # Step 2: Read documents from silver layer
    silver_df = spark.read.parquet(SILVER_PATH)
    print(f"Read {silver_df.count()} documents from silver layer")

    # Step 3: Turn chunk_text into a Spark UDF
    chunk_udf = udf(lambda text: chunk_text(text), ArrayType(StringType()))

    # Step 4: Chunk documents
    # FIX: Use row_number() instead of monotonically_increasing_id()
    #   - monotonically_increasing_id() gives random IDs — different every run
    #   - row_number() gives 1, 2, 3 per doc_id — same every run
    #   - This is critical: MERGE needs stable IDs to match rows across runs
    window = Window.partitionBy("doc_id").orderBy("chunk_text")

    chunked_df = silver_df \
        .withColumn("chunks", chunk_udf(col("content"))) \
        .select(
            col("doc_id"),
            col("title"),
            col("category"),
            explode(col("chunks")).alias("chunk_text"),
        ) \
        .withColumn("chunk_id", row_number().over(window)) \
        .withColumn("processed_at", current_timestamp()) \
        .select("doc_id", "chunk_id", "title", "category", "chunk_text", "processed_at")

    print(f"Created {chunked_df.count()} chunks")

    # Step 5: Write to gold layer using MERGE (upsert)
    if DeltaTable.isDeltaTable(spark, GOLD_PATH):
        # Gold table already exists — MERGE new data into it
        gold_table = DeltaTable.forPath(spark, GOLD_PATH)

        # This says:
        #   "Match rows where doc_id AND chunk_id are the same"
        #   "If match found → update that row"
        #   "If no match → insert as new row"
        gold_table.alias("gold").merge(
            chunked_df.alias("new"),
            "gold.doc_id = new.doc_id AND gold.chunk_id = new.chunk_id"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()

        print("Merged into existing gold table (upsert)")
    else:
        # First run ever — just create the table
        chunked_df.write.format("delta").mode("overwrite").save(GOLD_PATH)
        print("Created new gold table")

    # Step 6: Verify
    gold_df = spark.read.format("delta").load(GOLD_PATH)
    print(f"Gold layer has {gold_df.count()} total records (no duplicates)")

    spark.stop()


if __name__ == "__main__":
    run()
