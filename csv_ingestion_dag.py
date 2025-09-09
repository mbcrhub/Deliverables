from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from datetime import datetime

def ingest_csv_to_postgres():
    # Load CSV from GitHub or public URL
    df = pd.read_csv("https://raw.githubusercontent.com/yourusername/yourrepo/main/sample.csv")

    # Connect to Postgres
    hook = PostgresHook(postgres_conn_id="postgres_default")
    engine = hook.get_sqlalchemy_engine()

    # Write to table
    df.to_sql("raw_data", engine, if_exists="replace", index=False)

default_args = {
    "start_date": datetime(2023, 1, 1),
}

with DAG(
    dag_id="csv_ingestion_dag",
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
) as dag:

    ingest_task = PythonOperator(
        task_id="ingest_csv",
        python_callable=ingest_csv_to_postgres,
    )
