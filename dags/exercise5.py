"""Example DAG demonstrating the usage of the BashOperator."""

from datetime import timedelta
from datetime import datetime

import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator

args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(2),
}

dag = DAG(
    dag_id='my_fifth_dag',
    default_args=args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60),
)

the_end = DummyOperator(
    task_id='the_end',
    dag=dag,
    trigger_rule='none_failed',
)


def printWeekDate():
    """This is a function that will run within the DAG execution"""
    print(datetime.today().weekday())
    return datetime.today().weekday()


def return_branch(**kwargs):
    weekday = printWeekDate()
    if weekday == 0:
        return 'sendToBob'
    if weekday == 1:
        return 'sendToJoe'
    if weekday == 2:
        return 'sendToAlice'

    return 'sendToBob'


print_date = PythonOperator(
    task_id="print_week_date",
    python_callable=printWeekDate,
    dag=dag,
)

branching = BranchPythonOperator(
    task_id='branching',
    python_callable=return_branch,
    provide_context=True,
    dag=dag,
)

print_date >> branching

for i in ['Bob', 'Alice', 'Joe']:
    send = DummyOperator(
        task_id=f'sendTo{i}',
        dag=dag,
    )



    branching >> send >> the_end
