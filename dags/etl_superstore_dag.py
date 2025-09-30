import glob
import os
import shutil

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

DATA_DIR = "/opt/airflow/data/"
ARCHIVE_DIR = os.path.join(DATA_DIR, "archive/")
BATCH_PATTERN = "batch_*.csv"

# SQL
USER_INSERT_SQL = """
    INSERT INTO users (customer_id, segment, region, state, city)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (customer_id) DO NOTHING
"""
PRODUCT_INSERT_SQL = """
    INSERT INTO products (product_id, category, sub_category, product_name)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (product_id) DO NOTHING
"""
ORDER_INSERT_SQL = """
    INSERT INTO orders (
        order_id, order_date, ship_date, ship_mode,
        customer_id, product_id, sales, quantity, discount, profit
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

# EXTRACT FUNCTIONS
def extract_users(df):
    users_df = df.rename(columns={
        'Customer ID': 'customer_id',
        'Segment': 'segment',
        'Region': 'region',
        'State': 'state',
        'City': 'city'
    })[['customer_id','segment','region','state','city']].drop_duplicates()

    return [(
        row['customer_id'], row['segment'], row['region'], row['state'], row['city']
    ) for _, row in users_df.iterrows()]
def extract_products(df):
    products_df = df.rename(columns={
        'Product ID': 'product_id',
        'Category': 'category',
        'Sub-Category': 'sub_category',
        'Product Name': 'product_name'
    })[['product_id', 'category', 'sub_category', 'product_name']].drop_duplicates()

    return [(
        row['product_id'], row['category'], row['sub_category'], row['product_name']
    ) for _, row in products_df.iterrows()]
def extract_orders(df):
    df = df.rename(columns={
        'Order ID': 'order_id',
        'Order Date': 'order_date',
        'Ship Date': 'ship_date',
        'Ship Mode': 'ship_mode',
        'Customer ID': 'customer_id',
        'Product ID': 'product_id',
        'Sales': 'sales',
        'Quantity': 'quantity',
        'Discount': 'discount',
        'Profit': 'profit'
    })

    df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')
    df['ship_date'] = pd.to_datetime(df['ship_date'], errors='coerce')

    return [(
        row['order_id'], row['order_date'], row['ship_date'],
        row['ship_mode'], row['customer_id'], row['product_id'],
        row['sales'], row['quantity'], row['discount'], row['profit']
    ) for _, row in df.iterrows()]

def process_all_tables():
    os.makedirs(ARCHIVE_DIR, exist_ok=True)
    batch_files = glob.glob(os.path.join(DATA_DIR, BATCH_PATTERN))

    if not batch_files:
        logging.info("⚠️ No batch files found.")
        return

    hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = hook.get_conn()
    cursor = conn.cursor()

    for batch_file in batch_files:
        file_name = os.path.basename(batch_file)
        archived_path = os.path.join(ARCHIVE_DIR, file_name)

        if os.path.exists(archived_path):
            logging.info(f"⏩ Skipping already processed file: {file_name}")
            continue

        df = pd.read_csv(batch_file)

        # USERS
        users_rows = extract_users(df)
        for row in users_rows:
            cursor.execute(USER_INSERT_SQL, row)

        # PRODUCTS
        products_rows = extract_products(df)
        for row in products_rows:
            cursor.execute(PRODUCT_INSERT_SQL, row)

        # ORDERS
        orders_rows = extract_orders(df)
        for row in orders_rows:
            cursor.execute(ORDER_INSERT_SQL, row)

        conn.commit()
        shutil.move(batch_file, archived_path)
        logging.info(f"✅ Inserted all tables & archived file: {file_name}")

    cursor.close()
    conn.close()

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

    create_tables >> insert_all_task