from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from datetime import datetime
import pandas as pd
import sqlite3

DB_PATH = "/tmp/weather.db"
CSV_PATH = "/root/airflow/data/weatherHistory.csv"

def load_weather_data():
    df = pd.read_csv(CSV_PATH)
    # Clean column names to be SQL-safe
    df.columns = (
        df.columns
        .str.strip()
        .str.replace('[^A-Za-z0-9_]+', '', regex=True)
        .str.replace('C', 'Celsius', regex=False)
    )
    print("✅ Cleaned columns:", df.columns.tolist())

    conn = sqlite3.connect(DB_PATH)
    df.to_sql("weather", conn, if_exists="replace", index=False)
    conn.close()
    print(f"✅ Loaded {len(df)} rows into 'weather' table")

def show_summary():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT * FROM weather LIMIT 5;", conn)
    print(df)
    conn.close()

with DAG(
    dag_id="weather_sql_pipeline",
    start_date=datetime(2025, 10, 7),
    schedule="@daily",
    catchup=False,
    tags=["weather", "sql", "etl"],
) as dag:

    load_data = PythonOperator(
        task_id="load_data",
        python_callable=load_weather_data,
    )

    add_tempdiff_column = SqliteOperator(
        task_id="add_tempdiff_column",
        sqlite_conn_id="sqlite_weather",
        sql="ALTER TABLE weather ADD COLUMN TempDiff REAL;",
    )

    update_tempdiff_values = SqliteOperator(
        task_id="update_tempdiff_values",
        sqlite_conn_id="sqlite_weather",
        sql="""
        UPDATE weather
        SET TempDiff = TemperatureCelsius - ApparentTemperatureCelsius;
        """,
    )

    show_results = PythonOperator(
        task_id="show_results",
        python_callable=show_summary,
    )

    # Task order
    load_data >> add_tempdiff_column >> update_tempdiff_values >> show_results
