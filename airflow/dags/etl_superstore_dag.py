from datetime import timedelta, datetime
import os
import psycopg2
import pandas as pd
from airflow.providers.standard.operators.python import PythonOperator

from airflow import DAG

default_args = {
    'owner': 'suyeon',
    'retries': 1,
    'retry_delay': timedelta(minutes = 1)
}

def insert_csv_to_postgres():
    import glob

    csv_dir = '/opt/airflow/data'
    csv_files = sorted(glob.glob(os.path.join(csv_dir, 'batch_*.csv')))

    if not csv_files:
        print("❌ No CSV files found.")
        return

    conn = psycopg2.connect(
        host = 'postgres',
        database = 'airflow',
        user = 'airflow',
        password = 'airflow'
    )
    cursor = conn.cursor()

    for file_path in csv_files:
        print(f"🚀 Inserting data from {file_path}")
        df = pd.read_csv(file_path)

        for _, row in df.iterrows():
            cursor.execute("""
                INSERT INTO superstore_sales (
                    order_id, order_date, ship_date, ship_mode,
                    customer_id, customer_name, segment, country,
                    city, state, postal_code, region, product_id,
                    category, sub_category, product_name, sales,
                    quantity, discount, profit
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row['Order ID'], row['Order Date'], row['Ship Date'], row['Ship Mode'],
                row['Customer ID'], row['Customer Name'], row['Segment'], row['Country'],
                row['City'], row['State'], row['Postal Code'], row['Region'], row['Product ID'],
                row['Category'], row['Sub-Category'], row['Product Name'], row['Sales'],
                row['Quantity'], row['Discount'], row['Profit']
            ))

        conn.commit()
        print(f"✅ Inserted {len(df)} rows from {file_path}")

    cursor.close()
    conn.close()

with DAG(
    dag_id='etl_superstore_dag',
    default_args=default_args,
    start_date=datetime(2025, 9, 1),
    schedule_interval='@hourly',
    catchup=False
) as dag:
    task_insert_data = PythonOperator(
        task_id='insert_csv_to_postgres',
        python_callable=insert_csv_to_postgres
    )

    task_insert_data