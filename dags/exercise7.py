import airflow
from airflow.models import DAG

from launch_to_gcs_operator import LaunchToGCSOperator

args = {"owner": "godatadriven", "start_date": airflow.utils.dates.days_ago(1)}
dag = DAG(
    dag_id="download_rocket_launches_exercise",
    default_args=args,
    description="DAG downloading rocket launches from Launch Library.",
    schedule_interval="0 0 * * *",
)

download_rocket_launches = LaunchToGCSOperator(
    first_date=airflow.utils.dates.days_ago(10),
    end_date=airflow.utils.dates.days_ago(1),
    task_id="download_rocket_launches",
    provide_context=True,
    dag=dag,
)

download_rocket_launches
