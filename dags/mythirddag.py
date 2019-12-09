
"""Example DAG demonstrating the usage of the BashOperator."""

from datetime import timedelta

import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(2),
}

dag = DAG(
    dag_id='my_second_dag',
    default_args=args,
    schedule_interval=timedelta(minutes=150),
    dagrun_timeout=timedelta(minutes=60),
)

run_this_last = DummyOperator(
    task_id='run_this_last',
    dag=dag,
)

# [START howto_operator_bash]
run_this1 = BashOperator(
    task_id='echo_1',
    bash_command='echo 1',
    dag=dag,
)

# [START howto_operator_bash]
run_this2 = BashOperator(
    task_id='echo_2',
    bash_command='echo 2',
    dag=dag,
)

# [START howto_operator_bash]
run_this3 = BashOperator(
    task_id='echo_3',
    bash_command='echo 3',
    dag=dag,
)

# [START howto_operator_bash]
run_this4 = BashOperator(
    task_id='echo4',
    bash_command='echo 4',
    dag=dag,
)
# [END howto_operator_bash]


# for i in range(3):
#     task = BashOperator(
#         task_id='runme_' + str(i),
#         bash_command='echo "{{ task_instance_key_str }}" && sleep 1',
#         dag=dag,
#     )
#     task >> run_this

# [START howto_operator_bash_template]
also_run_this = BashOperator(
    task_id='also_run_this',
    bash_command='echo "run_id={{ run_id }} | dag_run={{ dag_run }}"',
    dag=dag,
)
# [END howto_operator_bash_template]

run_this1 >> run_this2 >> [run_this3, run_this4] >> also_run_this

