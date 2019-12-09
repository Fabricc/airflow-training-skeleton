
"""Example DAG demonstrating the usage of the BashOperator."""

from datetime import timedelta

import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(2),
}

dag = DAG(
    dag_id='my_fourth_dag',
    default_args=args,
    schedule_interval=timedelta(minutes=150),
    dagrun_timeout=timedelta(minutes=60),
)

the_end = DummyOperator(
    task_id='run_this_last',
    dag=dag
)


def printExecutionDate(date):
    """This is a function that will run within the DAG execution"""
    print(date)

print_date = PythonOperator(
    task_id = "printDate",
    python_callable=printExecutionDate,
    op_kwargs={'date': '{{execution_date}}}'},
    dag=dag,
)

for i in [1, 5, 10]:
    sleep = BashOperator(
        task_id='sleep',
        bash_command=f'sleep({i})',
        dag=dag,
    )
    sleep >> the_end
# [END howto_operator_bash_template]

print_date >> sleep

