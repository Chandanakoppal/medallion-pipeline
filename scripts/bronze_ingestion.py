"""
Bronze Layer — Raw Ingestion (with File Tracking)
===================================================
WHY: The Bronze layer is your "landing zone." You ingest raw data exactly as-is,
with zero transformations. This preserves the original source of truth.

IMPROVEMENT: This version tracks which files have already been ingested.
On subsequent runs, only NEW files get processed. This is how production
pipelines work — you never re-ingest data you've already seen.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, lit, col
from datetime import datetime
import glob
import os


def get_spark():
    """Create a SparkSession with Delta Lake enabled."""
    return (
        SparkSession.builder
        .appName("BronzeIngestion")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .master("local[*]")
        .getOrCreate()
    )


def standardize_columns(df):
    """
    Cast all columns to consistent types.
    NYC taxi parquet files use different types across months.
    """
    for c in df.columns:
        df = df.withColumnRenamed(c, c.lower())

    long_cols = ["vendorid", "pulocationid", "dolocationid", "payment_type"]
    double_cols = [
        "passenger_count", "trip_distance", "ratecodeid",
        "fare_amount", "extra", "mta_tax", "tip_amount",
        "tolls_amount", "improvement_surcharge", "total_amount",
        "congestion_surcharge", "airport_fee"
    ]

    for c in long_cols:
        if c in df.columns:
            df = df.withColumn(c, col(c).cast("long"))

    for c in double_cols:
        if c in df.columns:
            df = df.withColumn(c, col(c).cast("double"))

    return df


def get_already_ingested_files(spark, bronze_path):
    """
    Check which files have already been ingested by reading the Bronze table's
    _source_file column.

    WHY this approach? The metadata we stamp on every row during ingestion now
    serves double duty — it's both an audit trail AND an ingestion tracker.
    No separate tracking table needed.

    RETURNS: A set of filenames (just the filename, not the full path) that
    have already been ingested.
    """
    try:
        bronze_df = spark.read.format("delta").load(bronze_path)
        ingested = (
            bronze_df
            .select("_source_file")
            .distinct()
            .collect()
        )
        ingested_filenames = set()
        for row in ingested:
            if row["_source_file"]:
                filename = os.path.basename(row["_source_file"])
                ingested_filenames.add(filename)

        print(f"Already ingested files: {ingested_filenames}")
        return ingested_filenames

    except Exception:
        # Bronze table doesn't exist yet — first run
        print("No existing Bronze table found — first run, ingesting all files.")
        return set()


def run_bronze():
    """Main Bronze ingestion function — called by Airflow."""

    spark = get_spark()

    RAW_PATH = "/opt/airflow/data/raw/"
    BRONZE_PATH = "/opt/airflow/data/bronze/yellow_taxi"

    # --- Find all raw files ---
    all_files = glob.glob(f"{RAW_PATH}*.parquet")
    print(f"Found {len(all_files)} raw files: {[os.path.basename(f) for f in all_files]}")

    # --- Filter out already-ingested files ---
    # WHY? Without this check, every run re-ingests everything, creating
    # duplicates in Bronze. With tracking, we only process NEW files.
    # This is the simplest production-grade incremental pattern.
    already_ingested = get_already_ingested_files(spark, BRONZE_PATH)

    new_files = [
        f for f in all_files
        if os.path.basename(f) not in already_ingested
    ]

    if not new_files:
        print("No new files to ingest. Bronze is up to date.")
        spark.stop()
        return

    print(f"New files to ingest: {[os.path.basename(f) for f in new_files]}")

    # --- Read each new file, standardize, then union ---
    dfs = []
    for f in new_files:
        file_df = spark.read.parquet(f)
        file_df = standardize_columns(file_df)
        dfs.append(file_df)

    df = dfs[0]
    for other in dfs[1:]:
        df = df.unionByName(other, allowMissingColumns=True)

    print(f"New records to ingest: {df.count()}")

    # --- Add metadata columns ---
    batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")

    df = (
        df
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", input_file_name())
        .withColumn("_batch_id", lit(batch_id))
    )

    # --- Write to Delta (append mode) ---
    df.write.format("delta").mode("append").save(BRONZE_PATH)

    # --- Validate ---
    bronze_df = spark.read.format("delta").load(BRONZE_PATH)
    bronze_count = bronze_df.count()
    print(f"Total Bronze record count: {bronze_count}")
    print(f"Bronze columns: {bronze_df.columns}")
    print("Bronze ingestion complete.")

    spark.stop()


if __name__ == "__main__":
    run_bronze()
