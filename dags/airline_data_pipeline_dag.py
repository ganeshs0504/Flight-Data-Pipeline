from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',  # This field is mandatory
    'retries': 0,
}
with DAG(dag_id="pipeline_dag6",
         description="First dag1",
         default_args = default_args,
         start_date=datetime(2024, 10, 17),
         schedule_interval='@daily',
         ) as dag:
    task1 = BashOperator(
        task_id='raw_to_structured',
        bash_command="echo Hello_world!!!!"
    )

    task2 = BashOperator(
        task_id='testing1',
        bash_command="sleep 5"
    )

    task1 >> task2