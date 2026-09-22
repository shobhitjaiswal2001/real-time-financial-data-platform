from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


PROJECT_DIR = "/opt/project"
DBT_DIR = f"{PROJECT_DIR}/financial_dbt"

default_args = {
    "owner": "shobhit",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "email": ["martinjais01@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
}

with DAG(
    dag_id="financial_data_pipeline",
    default_args=default_args,
    description="End-to-end financial data pipeline: ingest -> bronze -> silver -> gold -> postgres -> dbt",
    schedule_interval="10 12 * * *",  # daily at 12:10 PM, matches your existing Task Scheduler time
    start_date=datetime(2026, 9, 1),
    catchup=False,
    tags=["financial-data-platform"],
) as dag:

    ingest = BashOperator(
        task_id="ingest_alpha_vantage",
        bash_command=f"cd {PROJECT_DIR} && python src/ingestion/alpha_vantage.py",
    )

    bronze = BashOperator(
        task_id="json_to_parquet",
        bash_command=f"cd {PROJECT_DIR} && python src/transformation/json_to_parquet.py",
    )

    silver = BashOperator(
        task_id="spark_silver",
        bash_command=f"cd {PROJECT_DIR} && python src/transformation/spark_silver.py",
    )

    gold = BashOperator(
        task_id="spark_gold",
        bash_command=f"cd {PROJECT_DIR} && python src/transformation/spark_gold.py",
    )

    postgres_load = BashOperator(
        task_id="postgres_load",
        bash_command=f"cd {PROJECT_DIR} && python src/warehouse/postgres_loader.py",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && dbt run --profiles-dir {DBT_DIR}",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && dbt test --profiles-dir {DBT_DIR}",
    )

    # Define the dependency chain
    ingest >> bronze >> silver >> gold >> postgres_load >> dbt_run >> dbt_test