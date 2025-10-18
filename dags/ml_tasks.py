import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

def generate_sales_timeseries():
    input_path = "/opt/airflow/data/orders.csv"
    output_path = "/opt/airflow/data/sales_timeseries.csv"

    if not os.path.exists(input_path):
        print("❌ orders.csv not found.")
        return

    df = pd.read_csv(input_path)

    df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')

    df_grouped = df.groupby(df['order_date'].dt.date)['sales'].sum().reset_index()
    df_grouped.columns = ['date', 'total_sales'] # 날짜, 총 매출액
    df_grouped['date'] = pd.to_datetime(df_grouped['date'], errors='coerce')

    df_grouped['day_of_week'] = df_grouped['date'].dt.dayofweek # 요일
    df_grouped['month'] = df_grouped['date'].dt.month # 월
    df_grouped['is_weekend'] = df_grouped['day_of_week'].isin([4, 5, 6]).astype(int) # 주말 여부

    df_grouped.sort_values(by='date', inplace=True)
    df_grouped['sales_lag_1'] = df_grouped['total_sales'].shift(1) # 전일 매출액 (1일 전의 total_sales 값)
    df_grouped['rolling_avg_3'] = df_grouped['total_sales'].rolling(window=3).mean() # 3일 이동평균 매출 (최근 3일간의 평균 매출)
    df_grouped['rolling_std_3'] = df_grouped['total_sales'].rolling(window=3).std() # 3일 이동 표준편차 (최근 3일간 매출의 변동성)

    # NaN
    df_grouped.dropna(inplace=True)

    df_grouped.to_csv(output_path, index=False)
    print(f"✅ Saved: {output_path}")

with DAG(
    dag_id="ml_tasks",
    schedule_interval="*/5 * * * *",
    start_date=datetime(2025, 9, 29, 0, 0),
    catchup=False,
    tags=['ml'],
) as dag:
    generate_sales_csv = PythonOperator(
        task_id="generate_sales_timeseries",
        python_callable=generate_sales_timeseries,
    )