"""
Generate sample silver-layer document data as parquet files.
Run this once to create test data.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from datetime import datetime

SILVER_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "silver")

SAMPLE_DOCS = [
    ("doc-001", "Introduction to Machine Learning", "Machine learning is a subset of artificial intelligence that enables systems to learn from data. It uses algorithms to find patterns in large datasets. Common approaches include supervised learning, unsupervised learning, and reinforcement learning. Applications range from image recognition to natural language processing.", "technology", "2026-01-15"),
    ("doc-002", "Financial Planning Basics", "A solid financial plan starts with budgeting and saving. Emergency funds should cover three to six months of expenses. Investing early takes advantage of compound interest. Diversification reduces portfolio risk across different asset classes.", "finance", "2026-02-10"),
    ("doc-003", "Climate Change Overview", "Global temperatures have risen by 1.1 degrees Celsius since pre-industrial times. The primary driver is greenhouse gas emissions from burning fossil fuels. Effects include rising sea levels, extreme weather events, and biodiversity loss. International agreements aim to limit warming to 1.5 degrees.", "science", "2026-03-05"),
    ("doc-004", "Healthy Eating Guide", "A balanced diet includes fruits, vegetables, whole grains, and lean proteins. Processed foods should be limited. Hydration is essential with at least eight glasses of water daily. Meal planning helps maintain consistent nutrition.", "health", "2026-01-20"),
    ("doc-005", "Remote Work Best Practices", "Set up a dedicated workspace free from distractions. Maintain regular working hours to separate work and personal life. Use video calls for important discussions. Take regular breaks to avoid burnout and maintain productivity.", "business", "2026-02-28"),
]


def generate():
    spark = SparkSession.builder \
        .appName("GenerateSilverData") \
        .master("local[*]") \
        .getOrCreate()

    schema = StructType([
        StructField("doc_id", StringType(), False),
        StructField("title", StringType(), False),
        StructField("content", StringType(), False),
        StructField("category", StringType(), False),
        StructField("created_date", StringType(), False),
    ])

    df = spark.createDataFrame(SAMPLE_DOCS, schema)
    df.write.mode("overwrite").parquet(SILVER_PATH)
    print(f"Wrote {df.count()} documents to {SILVER_PATH}")

    spark.stop()


if __name__ == "__main__":
    generate()
