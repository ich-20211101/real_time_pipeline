import pandas as pd
import os

import psycopg2

IS_DOCKER = os.path.exists('/.dockerenv')

DB_CONFIG = {
    'host': 'postgres' if IS_DOCKER else 'localhost',
    'port': 5432,
    'dbname': 'airflow',
    'user': 'airflow',
    'password': 'airflow'
}

TABLES = ['users', 'products', 'orders']

OUTPUT_DIR = '/opt/airflow/data' if IS_DOCKER else os.path.join(os.getcwd(), '../data')

def export_table_to_csv(conn, table_name):
    query = f"SELECT * FROM {table_name};"
    df = pd.read_sql(query, conn)

    if df.empty:
        print(f"⚠️ No data in table '{table_name}'. Skipping export.")
        return

    output_path = os.path.join(OUTPUT_DIR, f'{table_name}.csv')
    df.to_csv(output_path, index=False)
    print(f"✅ Exported '{table_name}' to {output_path}")

def main():
    conn = psycopg2.connect(**DB_CONFIG)
    for table in TABLES:
        export_table_to_csv(conn, table)
    conn.close()

if __name__ == '__main__':
    main()