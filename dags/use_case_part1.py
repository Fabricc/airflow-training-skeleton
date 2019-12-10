import airflow
from airflow.models import DAG

from http_to_gcs_operator import HttpToGcsOperator

args = {"owner": "godatadriven", "start_date": airflow.utils.dates.days_ago(1)}
dag = DAG(
    dag_id="track_prices",
    default_args=args,
    schedule_interval="0 0 * * *",
)

fetch_exchange_rates = HttpToGcsOperator(
    endpoint=f"/start_at={airflow.utils.dates.days_ago(1).date()}&end_at={airflow.utils.dates.days_ago(0).date()}&symbols=EUR&base=GBP",
    gcs_bucket="airflow_training_bucket",
    task_id="fetch_exchange_rates",
    gcs_path="bucket",
    method="GET",
    gcs_conn_id="google_cloud_default",
    dag=dag)

fetch_exchange_rates
