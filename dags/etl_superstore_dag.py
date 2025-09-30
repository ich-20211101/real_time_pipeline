import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

def print_hello():
    logging.info("✅ Hello Airflow logging")

def insert_data():
    hook = PostgresHook(postgre_conn_id = "postgres_default")
    conn = hook.get_conn()
    cursor = conn.cursor()

    df = pd.read_csv('/opt/airflow/dags/data/superstore_cleaned.csv')

with DAG(
    dag_id='etl_superstore_dag',
    schedule_interval="* * * * *",
    start_date=datetime(2025, 9, 29, 0, 0),
    catchup=False,
    tags=["test"],
) as dag:
    create_tables = PostgresOperator(
        task_id='create_tables',
        postgres_conn_id='postgres_default',  # ← 네가 설정한 연결 ID로 바꿔도 됨
        sql='sql/create_table.sql',
    )
    print_hello_task = PythonOperator(
        task_id='print_hello',
        python_callable=print_hello,
    )