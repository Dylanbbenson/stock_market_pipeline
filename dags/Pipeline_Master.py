from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os
import logging
import subprocess
from datetime import date, datetime
current_date = date.today().strftime('%Y-%m-%d')
current_time = datetime.now().strftime("%H")
csv_file = f"./data/stock_market_{current_date}_{current_time}.csv"

# DAG args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# DAG definition
dag = DAG(
    'snowflake_data_pipeline',
    default_args=default_args,
    description='data pipeline to Snowflake',
    schedule='0 * * * *',  # top of every hour
    start_date= datetime(2024, 8, 22),
    catchup=False,

)

#Funcs
def run_script(script_path, script_arg=None) -> None:
    command = [sys.executable, script_path]
    if script_arg is not None:
        command.append(script_arg)
    try:
        subprocess.run(command, check=True)
    except subprocess.CalledProcessError as e:
        logging.error(f"Error executing {script_path}: {e}")
        print(f"Error executing {script_path}: {e}")

def scrape_tickers() -> None:
    logging.info("Starting ETL Process...")
    if not os.path.exists(f"./data/tickers_{current_date}.json"):
        run_script('./src/scrape_tickers.py')
    logging.info("Tickers successfully scraped.")

def get_data() -> None:
    logging.info("Starting ETL Process...")
    run_script('./src/ingest_stock_market_data.py')
    logging.info("Data retrieval completed.")

def upload_to_snowflake() -> None:
    logging.info("Starting ETL Process...")
    run_script('./src/upload_to_snowflake.py')
    logging.info("Data successfully loaded to Snowflake.")

# Tasks
scrape_tickers_task = PythonOperator(
    task_id='scrape_tickers',
    python_callable=scrape_tickers,
    dag=dag
)

get_data_task = PythonOperator(
    task_id='get_data',
    python_callable=get_data,
    dag=dag
)

upload_to_snowflake_task = PythonOperator(
    task_id='upload_to_snowflake',
    python_callable=upload_to_snowflake,
    dag=dag,
)

#Workflow
scrape_tickers_task >> get_data_task >> upload_to_snowflake_task
