from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from common.analysis.main import TransactionAnalysis

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 20),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 22)
}

dag = DAG(
    'extract_transform_excel',
    default_args=default_args,
    description='Extract, Transform, and Load to Postgres',
    schedule_interval='0 */2 * * *',  # run every 2 hours
    catchup=False,
)

start_task = DummyOperator(task_id='start', dag=dag)
end_task = DummyOperator(task_id='end', dag=dag)


TransactionAnalysis_task = BashOperator(
    task_id='run_python_operator',
    bash_command="python3 /usr/local/airflow/dags/common/analysis/main.py",
    dag=dag
)

start_task >> TransactionAnalysis_task >> end_task
