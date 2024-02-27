from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

# These args will get passed on to each operator
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Define your DAG
dag = DAG(
    'run_shell_script',
    default_args=default_args,
    description='DAG to run a shell script for spotify project every 12 hours',
    schedule_interval=timedelta(hours=12),
    start_date=days_ago(1),
    catchup=False,
)

# Define the task to run your shell script
run_script = BashOperator(
    task_id='run_my_shell_script',
    bash_command='sh /Users/irerielunicornio/Documents/USF/Spring1/Distributed-Data-Systems/Final\ Project/scripts/main_script.sh ',
    dag=dag,
)

