"""
Medallion Pipeline DAG
========================
WHY Airflow? You have 4 scripts that must run in a specific order:
Bronze → Silver → Quality Checks → Gold. Running them manually works for testing,
but in production you need: scheduling, dependency management, retries on failure,
logging, and alerting. That's what Airflow does.

WHY a DAG? DAG = Directed Acyclic Graph. It defines tasks and their dependencies.
"Directed" = tasks flow one way. "Acyclic" = no circular dependencies.
Airflow reads this file, builds the graph, and executes tasks in order.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Add scripts folder to Python path so we can import our modules
sys.path.insert(0, "/opt/airflow/scripts")


# =========================================================================
# Task wrapper functions
# =========================================================================
# WHY wrappers? Each PythonOperator needs a callable. We import and call
# the run_ functions from our scripts. The wrapper pattern keeps the DAG
# file thin — all logic lives in the scripts, not here.

def task_bronze():
    """Run Bronze ingestion."""
    from bronze_ingestion import run_bronze
    run_bronze()


def task_silver():
    """Run Silver transformation."""
    from silver_transformation import run_silver
    run_silver()


def task_quality_checks():
    """Run data quality checks on Silver."""
    from data_quality_checks import run_checks
    run_checks()


def task_gold():
    """Run Gold aggregations."""
    from gold_aggregation import run_gold
    run_gold()


# =========================================================================
# DAG definition
# =========================================================================

# WHY these default_args?
# - owner: shows who owns this pipeline in the Airflow UI
# - retries: if a task fails (network blip, memory spike), retry once before
#   marking it as failed. Production pipelines often set 2-3 retries.
# - retry_delay: wait 5 minutes between retries to let transient issues resolve
default_args = {
    "owner": "chandana",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="medallion_pipeline",

    # WHY this start_date? Airflow needs a reference point. It doesn't
    # retroactively run for past dates because catchup=False.
    start_date=datetime(2024, 1, 1),

    # WHY @daily? This pipeline processes daily taxi data. In production,
    # you'd match the schedule to when new source data arrives.
    schedule_interval="@daily",

    # WHY catchup=False? Without this, Airflow would try to run the DAG
    # for every day since start_date (hundreds of runs). We only want
    # future scheduled runs + manual triggers.
    catchup=False,

    # Tags help filter DAGs in the UI when you have dozens of pipelines
    tags=["data-engineering", "portfolio", "medallion"],

    default_args=default_args,

    # Markdown description shown in the Airflow UI
    doc_md="""
    ## Medallion Pipeline
    End-to-end data pipeline using the Medallion Architecture pattern.

    **Layers:**
    - **Bronze**: Raw ingestion from NYC taxi parquet files
    - **Silver**: Cleaned, deduplicated, type-cast, validated
    - **Quality Checks**: Null checks, dedup checks, row count validation
    - **Gold**: Business aggregations (daily summary, location stats, monthly trends)
    """,

) as dag:

    bronze_task = PythonOperator(
        task_id="bronze_ingestion",
        python_callable=task_bronze,
    )

    silver_task = PythonOperator(
        task_id="silver_transformation",
        python_callable=task_silver,
    )

    quality_task = PythonOperator(
        task_id="data_quality_checks",
        python_callable=task_quality_checks,
    )

    gold_task = PythonOperator(
        task_id="gold_aggregation",
        python_callable=task_gold,
    )

    # =========================================================================
    # Task dependencies
    # =========================================================================
    # WHY this order? Each layer depends on the previous one's output.
    # The >> operator sets execution order. If Silver fails, Quality Checks
    # and Gold never run — this prevents bad data from propagating.
    #
    # In the Airflow UI, you'll see this as a left-to-right chain:
    # [Bronze] → [Silver] → [Quality Checks] → [Gold]
    bronze_task >> silver_task >> quality_task >> gold_task
