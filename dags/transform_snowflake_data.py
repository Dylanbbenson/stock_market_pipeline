from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'dbt_pipeline',
    default_args=default_args,
    description='run dbt',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
)

# Define the dbt run task
run_dbt_task = BashOperator(
    task_id='run_dbt',
    bash_command='dbt run',
    dag=dag,
)

# Define the dbt test task (optional)
test_dbt_task = BashOperator(
    task_id='test_dbt',
    bash_command='dbt test',
    dag=dag,
)

# Set task dependencies
run_dbt_task >> test_dbt_task
