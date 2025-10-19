from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator

from scripts.export_tables_to_csv import main as export_main
from scripts.extract_tables_processor import process_all_tables

with DAG(
    dag_id='etl_superstore_dag',
    schedule_interval="*/5 * * * *",
    start_date=datetime(2025, 9, 29, 0, 0),
    catchup=False,
    tags=["etl"],
) as dag:
    create_tables = PostgresOperator(
        task_id = 'create_tables',
        postgres_conn_id = 'postgres_default',
        sql = 'sql/create_table.sql',
    )
    insert_all_task = PythonOperator(
        task_id='insert_all_tables_from_batches',
        python_callable=process_all_tables,
    )
    export_to_csv_task = PythonOperator(
        task_id='export_tables_to_csv',
        python_callable=export_main,
    )

    create_tables >> insert_all_task >> export_to_csv_task