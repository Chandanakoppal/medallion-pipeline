"""
Data Quality Checks
====================
WHY: A pipeline that doesn't validate its output is a pipeline you can't trust.
These checks run AFTER Silver writes and BEFORE Gold reads. If any check fails,
Airflow marks the task as failed — so bad data never reaches your business tables.

PATTERN: Each check is a reusable function that takes a DataFrame and either
passes silently or raises an exception. This is a lightweight version of what
frameworks like Great Expectations do.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


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

# =========================================================================
# Reusable check functions
# =========================================================================

def check_not_empty(df, table_name):
    """
    WHY: The most basic check. If your table is empty after transformation,
    something went very wrong — maybe the source was missing, or your
    filter was too aggressive.
    """
    count = df.count()
    assert count > 0, f"FAIL: {table_name} is empty (0 rows)"
    print(f"  PASS: {table_name} has {count:,} rows")
    return count


def check_no_nulls(df, columns, table_name):
    """
    WHY: Critical columns should never be null after Silver cleaning.
    If they are, your cleaning logic has a gap.
    """
    for c in columns:
        null_count = df.filter(col(c).isNull()).count()
        assert null_count == 0, (
            f"FAIL: {table_name}.{c} has {null_count:,} nulls"
        )
        print(f"  PASS: {table_name}.{c} has no nulls")


def check_no_duplicates(df, key_columns, table_name):
    """
    WHY: After deduplication, the composite key should be unique.
    Duplicates here mean your dedup logic missed a case.
    """
    total = df.count()
    distinct = df.dropDuplicates(key_columns).count()
    dupes = total - distinct
    assert dupes == 0, (
        f"FAIL: {table_name} has {dupes:,} duplicate rows on {key_columns}"
    )
    print(f"  PASS: {table_name} has no duplicates on {key_columns}")


def check_min_row_count(df, min_expected, table_name):
    """
    WHY: A sanity check. You know roughly how many rows to expect.
    If the count drops way below that, something upstream broke.
    This catches silent failures like empty source files.
    """
    count = df.count()
    assert count >= min_expected, (
        f"FAIL: {table_name} has {count:,} rows, expected at least {min_expected:,}"
    )
    print(f"  PASS: {table_name} has {count:,} rows (>= {min_expected:,} expected)")


def check_no_negative_values(df, columns, table_name):
    """
    WHY: Fares, distances, and counts should never be negative after cleaning.
    Negative values usually indicate data corruption or bad source records
    that should have been quarantined.
    """
    for c in columns:
        neg_count = df.filter(col(c) < 0).count()
        assert neg_count == 0, (
            f"FAIL: {table_name}.{c} has {neg_count:,} negative values"
        )
        print(f"  PASS: {table_name}.{c} has no negative values")


def check_column_exists(df, columns, table_name):
    """
    WHY: Schema drift — if an upstream change drops or renames a column,
    you want to catch it before downstream queries break with cryptic errors.
    """
    for c in columns:
        assert c in df.columns, (
            f"FAIL: {table_name} is missing expected column '{c}'"
        )
        print(f"  PASS: {table_name} has column '{c}'")


# =========================================================================
# Main check runner
# =========================================================================

def run_checks():
    """Run all quality checks on the Silver table. Called by Airflow."""

    spark = get_spark()

    SILVER_PATH = "/opt/airflow/data/silver/yellow_taxi_clean"
    silver_df = spark.read.format("delta").load(SILVER_PATH)

    print("=" * 60)
    print("RUNNING DATA QUALITY CHECKS ON SILVER TABLE")
    print("=" * 60)

    # 1. Table not empty
    print("\n--- Row count check ---")
    check_not_empty(silver_df, "silver.yellow_taxi_clean")

    # 2. Minimum row count (3 months of NYC taxi ~ 8M+ trips)
    print("\n--- Minimum row count check ---")
    check_min_row_count(silver_df, 1_000_000, "silver.yellow_taxi_clean")

    # 3. No nulls in critical columns
    print("\n--- Null checks ---")
    critical_columns = [
        "vendor_id", "pickup_datetime", "dropoff_datetime",
        "pickup_location_id", "dropoff_location_id"
    ]
    check_no_nulls(silver_df, critical_columns, "silver.yellow_taxi_clean")

    # 4. No duplicates on composite key
    print("\n--- Duplicate check ---")
    check_no_duplicates(
        silver_df,
        ["vendor_id", "pickup_datetime", "dropoff_datetime",
         "pickup_location_id", "dropoff_location_id"],
        "silver.yellow_taxi_clean"
    )

    # 5. No negative values in financial/distance columns
    print("\n--- Negative value checks ---")
    check_no_negative_values(
        silver_df,
        ["fare_amount", "total_amount", "trip_distance"],
        "silver.yellow_taxi_clean"
    )

    # 6. Expected columns exist
    print("\n--- Schema checks ---")
    expected_columns = [
        "vendor_id", "pickup_datetime", "dropoff_datetime",
        "passenger_count", "trip_distance", "fare_amount",
        "total_amount", "pickup_date", "pickup_year", "pickup_month",
        "_silver_processed_at"
    ]
    check_column_exists(silver_df, expected_columns, "silver.yellow_taxi_clean")

    print("\n" + "=" * 60)
    print("ALL DATA QUALITY CHECKS PASSED")
    print("=" * 60)

    spark.stop()


if __name__ == "__main__":
    run_checks()
