"""
Gold Layer — Business Aggregations
====================================
WHY: Silver is clean but granular — every single taxi trip. Business users and
dashboards don't want 9 million rows. They want answers: "What was total revenue
last Tuesday?" "Which pickup zones are busiest?" "How is revenue trending monthly?"

Gold tables are pre-aggregated, query-ready, and modeled around business questions.
Think of Gold as the tables that would power a BI dashboard.

PATTERN: Overwrite (full refresh). Each run rebuilds from Silver.
Production systems use MERGE for incremental — we include that as a stretch goal.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum, count, avg, max, min, round as spark_round,
    dense_rank, lag, when, lit
)
from pyspark.sql.window import Window


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

def build_daily_summary(silver_df):
    """
    GOLD TABLE 1: Daily summary
    ============================
    WHO uses this: Operations team, daily dashboards
    WHAT it answers: "How many trips happened today? What was total revenue?"

    WHY these metrics? They're the first thing any stakeholder asks about.
    Total trips = volume, total/avg revenue = financial health, avg distance =
    operational pattern.
    """
    daily = (
        silver_df
        .groupBy("pickup_date")
        .agg(
            count("*").alias("trip_count"),
            spark_round(sum("total_amount"), 2).alias("total_revenue"),
            spark_round(avg("total_amount"), 2).alias("avg_revenue"),
            spark_round(avg("trip_distance"), 2).alias("avg_distance"),
            spark_round(avg("tip_amount"), 2).alias("avg_tip"),
            sum(when(col("tip_amount") > 0, 1).otherwise(0)).alias("tipped_trips"),
            count("*").alias("total_trips"),
        )
        # WHY this derived column? Tip percentage is a common business metric.
        # Computing it here saves every dashboard from recalculating.
        .withColumn(
            "tip_percentage",
            spark_round(col("tipped_trips") / col("total_trips") * 100, 1)
        )
        .orderBy("pickup_date")
    )
    return daily


def build_location_summary(silver_df):
    """
    GOLD TABLE 2: Top pickup locations
    ====================================
    WHO uses this: City planners, fleet managers
    WHAT it answers: "Where do most trips start? Which zones generate the most revenue?"

    WHY rank? So dashboards can show "Top 10 zones" without re-sorting every time.
    The dense_rank window function assigns ranks with no gaps.
    """
    location = (
        silver_df
        .groupBy("pickup_location_id")
        .agg(
            count("*").alias("trip_count"),
            spark_round(sum("total_amount"), 2).alias("total_revenue"),
            spark_round(avg("total_amount"), 2).alias("avg_revenue"),
            spark_round(avg("trip_distance"), 2).alias("avg_distance"),
        )
    )

    # WHY window function for ranking? It's the SQL-standard way to rank rows
    # within a result set. dense_rank() gives 1,2,3 with no gaps even on ties.
    window = Window.orderBy(col("trip_count").desc())
    location = location.withColumn("rank_by_trips", dense_rank().over(window))

    window_rev = Window.orderBy(col("total_revenue").desc())
    location = location.withColumn("rank_by_revenue", dense_rank().over(window_rev))

    return location


def build_monthly_trends(silver_df):
    """
    GOLD TABLE 3: Monthly trends
    ==============================
    WHO uses this: Executives, finance team
    WHAT it answers: "Is revenue growing or declining month over month?"

    WHY lag()? The lag window function grabs the previous row's value, letting
    you compute month-over-month growth in a single pass. No self-joins needed.
    """
    monthly = (
        silver_df
        .groupBy("pickup_year", "pickup_month")
        .agg(
            count("*").alias("trip_count"),
            spark_round(sum("total_amount"), 2).alias("total_revenue"),
            spark_round(avg("total_amount"), 2).alias("avg_revenue"),
            spark_round(avg("passenger_count"), 1).alias("avg_passengers"),
        )
        .orderBy("pickup_year", "pickup_month")
    )

    # WHY lag for growth? It gets the previous month's revenue without a self-join.
    # growth_pct = (this_month - last_month) / last_month * 100
    window = Window.orderBy("pickup_year", "pickup_month")
    monthly = (
        monthly
        .withColumn("prev_month_revenue", lag("total_revenue").over(window))
        .withColumn(
            "revenue_growth_pct",
            when(
                col("prev_month_revenue").isNotNull(),
                spark_round(
                    (col("total_revenue") - col("prev_month_revenue"))
                    / col("prev_month_revenue") * 100,
                    1
                )
            )
        )
    )

    return monthly


def build_payment_type_summary(silver_df):
    """
    GOLD TABLE 4: Payment type breakdown
    ======================================
    WHO uses this: Finance team, business analysts
    WHAT it answers: "What percentage of trips are cash vs card vs dispute?"

    WHY? Payment mix affects revenue forecasting, fraud detection, and
    operational planning (cash handling costs, card processing fees).
    """
    # NYC taxi payment type codes
    payment_labels = {
        1: "Credit card",
        2: "Cash",
        3: "No charge",
        4: "Dispute",
        5: "Unknown",
        6: "Voided",
    }

    payment = (
        silver_df
        .groupBy("payment_type")
        .agg(
            count("*").alias("trip_count"),
            spark_round(sum("total_amount"), 2).alias("total_revenue"),
            spark_round(avg("tip_amount"), 2).alias("avg_tip"),
        )
        .orderBy(col("trip_count").desc())
    )

    # Map codes to readable labels
    mapping_expr = when(lit(False), lit(""))  # start with empty
    for code, label in payment_labels.items():
        mapping_expr = mapping_expr.when(col("payment_type") == code, lit(label))
    mapping_expr = mapping_expr.otherwise("Other")

    payment = payment.withColumn("payment_label", mapping_expr)

    return payment


def run_gold():
    """Main Gold aggregation function — called by Airflow."""

    spark = get_spark()

    SILVER_PATH = "/opt/airflow/data/silver/yellow_taxi_clean"
    GOLD_BASE = "/opt/airflow/data/gold"

    silver_df = spark.read.format("delta").load(SILVER_PATH)
    print(f"Silver record count: {silver_df.count()}")

    # --- Build and write each Gold table ---

    print("\n--- Building daily_summary ---")
    daily = build_daily_summary(silver_df)
    daily.write.format("delta").mode("overwrite").save(f"{GOLD_BASE}/daily_summary")
    daily.show(5, truncate=False)
    print(f"daily_summary rows: {daily.count()}")

    print("\n--- Building location_summary ---")
    location = build_location_summary(silver_df)
    location.write.format("delta").mode("overwrite").save(f"{GOLD_BASE}/location_summary")
    location.show(10, truncate=False)
    print(f"location_summary rows: {location.count()}")

    print("\n--- Building monthly_trends ---")
    monthly = build_monthly_trends(silver_df)
    monthly.write.format("delta").mode("overwrite").save(f"{GOLD_BASE}/monthly_trends")
    monthly.show(truncate=False)
    print(f"monthly_trends rows: {monthly.count()}")

    print("\n--- Building payment_type_summary ---")
    payment = build_payment_type_summary(silver_df)
    payment.write.format("delta").mode("overwrite").save(f"{GOLD_BASE}/payment_type_summary")
    payment.show(truncate=False)
    print(f"payment_type_summary rows: {payment.count()}")

    print("\nGold aggregation complete.")
    spark.stop()


if __name__ == "__main__":
    run_gold()
