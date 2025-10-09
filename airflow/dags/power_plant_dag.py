from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from datetime import datetime
import pandas as pd
import sqlite3
import os

# -------------------------
# Config
# -------------------------
DB_PATH = "/tmp/powerplant.db"  # SQLite DB path
CSV_PATH = "/root/airflow/data/Training_set_ccpp.csv"

# -------------------------
# SQL Statements
# -------------------------
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS power_plant_data (
    AT REAL,
    V REAL,
    AP REAL,
    RH REAL,
    PE REAL
);
"""

CREATE_AVG_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS avg_power AS
SELECT 
    ROUND(AT,0) AS AT_rounded,
    AVG(PE) AS avg_PE
FROM power_plant_data
GROUP BY AT_rounded;
"""

# -------------------------
# Python Callables
# -------------------------
def load_csv_to_db():
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"{CSV_PATH} not found")
    
    df = pd.read_csv(CSV_PATH)
    conn = sqlite3.connect(DB_PATH)
    df.to_sql("power_plant_data", conn, if_exists="replace", index=False)
    conn.close()
    print(f"Loaded {len(df)} rows into power_plant_data table")

def print_avg_power():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT * FROM avg_power LIMIT 10;", conn)
    print("Sample average PE by rounded AT:")
    print(df)
    conn.close()

# -------------------------
# DAG Definition
# -------------------------
with DAG(
    dag_id="power_plant_sql_pipeline",
    start_date=datetime(2025, 10, 6),
    schedule_interval=None,
    catchup=False,
    tags=["sql", "powerplant"],
) as dag:

    create_table = SqliteOperator(
        task_id="create_table",
        sqlite_conn_id="sqlite_powerplant",
        sql=CREATE_TABLE_SQL,
    )

    load_data = PythonOperator(
        task_id="load_data",
        python_callable=load_csv_to_db,
    )

    calc_avg = SqliteOperator(
        task_id="calc_avg",
        sqlite_conn_id="sqlite_powerplant",
        sql=CREATE_AVG_TABLE_SQL,
    )

    show_results = PythonOperator(
        task_id="show_results",
        python_callable=print_avg_power,
    )

    # DAG task order
    create_table >> load_data >> calc_avg >> show_results
