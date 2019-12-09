"""Example DAG demonstrating the usage of the BashOperator."""

from datetime import timedelta
from datetime import datetime

import airflow
from airflow.models import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.decorators import apply_defaults
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

postgresHook = PostgresToGoogleCloudStorageOperator(conn_name_attr='postgres_training_id',
                                                    sql='SELECT transfer_date FROM land_registry_price_paid_uk;',
                                                    google_cloud_storage_conn_id='airflow_training_bucket',
                                                    dag=dag)

postgresHook


