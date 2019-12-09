"""Example DAG demonstrating the usage of the BashOperator."""

from datetime import timedelta

import airflow
from airflow.models import DAG
from airflow.contrib.operators.postgres_to_gcs_operator import PostgresToGoogleCloudStorageOperator

args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(2),
}

dag = DAG(
    dag_id='hook_exercise',
    default_args=args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60),
)

go = PostgresToGoogleCloudStorageOperator(postgres_conn_id='postgres_training_id',
                                                    sql='SELECT transfer_date FROM land_registry_price_paid_uk LIMIT 30',
                                                    bucket='airflow_training_bucket',
                                                    filename='tst.json',
                                                    task_id='postgres_hook',
                                                    dag=dag)

go


