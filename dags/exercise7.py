import json
import pathlib
import posixpath
import airflow
import requests
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

from launch_to_gcs_operator import LaunchToGCSOperator

args = {"owner": "godatadriven", "start_date": airflow.utils.dates.days_ago(1)}
dag = DAG(
    dag_id="download_rocket_launchesExercise",
    default_args=args,
    description="DAG downloading rocket launches from Launch Library.",
    schedule_interval="0 0 * * *",
)

download_rocket_launches = LaunchToGCSOperator(
    start_date=airflow.utils.dates.days_ago(10),
    end_date=airflow.utils.dates.days_ago(1),
    task_id="download_rocket_launches",
    dag=dag,
)

download_rocket_launches
