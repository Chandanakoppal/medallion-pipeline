"""
Silver Layer — Cleaning and Conforming
=======================================
WHY: Bronze has raw, messy data — duplicates, nulls, bad types, invalid records.
Silver is your "single source of truth." It's clean, typed, deduplicated, and
validated. Downstream consumers (Gold, analysts, dashboards) should ONLY read
from Silver, never from Bronze.

PATTERN: Overwrite (or merge for incremental). Unlike Bronze's append-only,
Silver represents the CURRENT clean state of the data.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, current_timestamp, when, lit, year, month
)
from pyspark.sql.types import DecimalType


def get_spark():
    return (
        SparkSession.builder
        .appName("...")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .master("local[*]")
        .getOrCreate()
    )

def run_silver():
    """Main Silver transformation function — called by Airflow."""

    spark = get_spark()

    BRONZE_PATH = "/opt/airflow/data/bronze/yellow_taxi"
    SILVER_PATH = "/opt/airflow/data/silver/yellow_taxi_clean"
    QUARANTINE_PATH = "/opt/airflow/data/silver/yellow_taxi_quarantine"

    # =========================================================================
    # STEP 1: Read from Bronze
    # =========================================================================
    # WHY read from Bronze, not raw? Bronze is our stable landing zone with
    # consistent schema and metadata. Raw files might change or get deleted.
    bronze_df = spark.read.format("delta").load(BRONZE_PATH)
    print(f"Bronze record count: {bronze_df.count()}")

    # =========================================================================
    # STEP 2: Standardize column names
    # =========================================================================
    # WHY snake_case? It's the industry convention for data warehouses.
    # Consistent naming prevents bugs when writing SQL or PySpark downstream.
    # "VendorID" vs "vendorid" vs "vendor_id" — pick one and stick to it.
    column_mapping = {
        "vendorid": "vendor_id",
        "tpep_pickup_datetime": "pickup_datetime",
        "tpep_dropoff_datetime": "dropoff_datetime",
        "passenger_count": "passenger_count",
        "trip_distance": "trip_distance",
        "ratecodeid": "rate_code_id",
        "store_and_fwd_flag": "store_and_fwd_flag",
        "pulocationid": "pickup_location_id",
        "dolocationid": "dropoff_location_id",
        "payment_type": "payment_type",
        "fare_amount": "fare_amount",
        "extra": "extra",
        "mta_tax": "mta_tax",
        "tip_amount": "tip_amount",
        "tolls_amount": "tolls_amount",
        "improvement_surcharge": "improvement_surcharge",
        "total_amount": "total_amount",
        "congestion_surcharge": "congestion_surcharge",
        "airport_fee": "airport_fee",
    }

    silver_df = bronze_df
    for old_name, new_name in column_mapping.items():
        if old_name in bronze_df.columns:
            silver_df = silver_df.withColumnRenamed(old_name, new_name)

    # =========================================================================
    # STEP 3: Cast data types explicitly
    # =========================================================================
    # WHY explicit casts? Never trust inferred types. "passenger_count" came in
    # as DOUBLE (because some months had nulls, and Spark widens to Double).
    # We cast to Integer because you can't have 1.5 passengers.
    # Money columns get Decimal(10,2) for precision — never use Float for money.
    silver_df = (
        silver_df
        .withColumn("vendor_id", col("vendor_id").cast("integer"))
        .withColumn("passenger_count", col("passenger_count").cast("integer"))
        .withColumn("rate_code_id", col("rate_code_id").cast("integer"))
        .withColumn("payment_type", col("payment_type").cast("integer"))
        .withColumn("pickup_location_id", col("pickup_location_id").cast("integer"))
        .withColumn("dropoff_location_id", col("dropoff_location_id").cast("integer"))
        .withColumn("fare_amount", col("fare_amount").cast(DecimalType(10, 2)))
        .withColumn("tip_amount", col("tip_amount").cast(DecimalType(10, 2)))
        .withColumn("tolls_amount", col("tolls_amount").cast(DecimalType(10, 2)))
        .withColumn("total_amount", col("total_amount").cast(DecimalType(10, 2)))
        .withColumn("trip_distance", col("trip_distance").cast(DecimalType(10, 2)))
    )

    # =========================================================================
    # STEP 4: Add derived columns
    # =========================================================================
    # WHY derived columns? These are commonly needed for analysis and
    # partitioning. Computing them once in Silver saves every downstream
    # consumer from repeating the same logic.
    silver_df = (
        silver_df
        .withColumn("pickup_date", to_date(col("pickup_datetime")))
        .withColumn("pickup_year", year(col("pickup_datetime")))
        .withColumn("pickup_month", month(col("pickup_datetime")))
    )

    # =========================================================================
    # STEP 5: Filter invalid records to quarantine
    # =========================================================================
    # WHY quarantine instead of just dropping? You never want to silently lose
    # data. Bad records go to a separate table so you can investigate later.
    # Common issues: null IDs, zero-distance trips, negative fares, future dates.

    invalid_mask = (
        col("vendor_id").isNull()
        | col("pickup_datetime").isNull()
        | col("dropoff_datetime").isNull()
        | (col("trip_distance") <= 0)
        | (col("fare_amount") < 0)
        | (col("total_amount") < 0)
        | (col("passenger_count") <= 0)
        | (col("pickup_datetime") > current_timestamp())  # future dates
    )

    quarantine_df = silver_df.filter(invalid_mask)
    silver_df = silver_df.filter(~invalid_mask)

    quarantine_count = quarantine_df.count()
    print(f"Quarantined records: {quarantine_count}")

    # Write quarantine table
    if quarantine_count > 0:
        quarantine_df.write.format("delta").mode("overwrite").save(QUARANTINE_PATH)

    # =========================================================================
    # STEP 6: Deduplicate
    # =========================================================================
    # WHY deduplicate? Bronze is append-only — if you ran ingestion twice,
    # you have duplicate rows. We deduplicate on a composite key: the
    # combination of columns that uniquely identifies a trip.
    # WHY these columns? A trip is uniquely defined by who picked up,
    # when, where from, and where to.
    before_dedup = silver_df.count()
    silver_df = silver_df.dropDuplicates([
        "vendor_id", "pickup_datetime", "dropoff_datetime",
        "pickup_location_id", "dropoff_location_id"
    ])
    after_dedup = silver_df.count()
    print(f"Before dedup: {before_dedup}, After dedup: {after_dedup}")
    print(f"Duplicates removed: {before_dedup - after_dedup}")

    # =========================================================================
    # STEP 7: Drop Bronze metadata columns
    # =========================================================================
    # WHY drop these? Silver consumers don't need ingestion metadata.
    # Silver has its own processing timestamp.
    silver_df = silver_df.drop("_ingestion_timestamp", "_source_file", "_batch_id")

    # Add Silver processing timestamp
    silver_df = silver_df.withColumn("_silver_processed_at", current_timestamp())

    # =========================================================================
    # STEP 8: Write to Delta
    # =========================================================================
    # WHY overwrite? Silver represents the current clean state.
    # For production, you'd use MERGE (upsert) for incremental loads —
    # we'll add that as a stretch goal.
    silver_df.write.format("delta").mode("overwrite").save(SILVER_PATH)

    # --- Final validation ---
    final_df = spark.read.format("delta").load(SILVER_PATH)
    print(f"\nSilver record count: {final_df.count()}")
    print("Silver schema:")
    final_df.printSchema()
    print("\nSample data:")
    final_df.show(5, truncate=False)
    print("Silver transformation complete.")

    spark.stop()


if __name__ == "__main__":
    run_silver()
