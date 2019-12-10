import airflow
from airflow.models import DAG

from dags.http_to_gcs_operator import HttpToGcsOperator

args = {"owner": "godatadriven", "start_date": airflow.utils.dates.days_ago(1)}
dag = DAG(
    dag_id="track_prices",
    default_args=args,
    schedule_interval="0 0 * * *",
)

fetch_exchange_rates = HttpToGcsOperator(
    endpoint=f"https://api.exchangeratesapi.io/history?start_at={airflow.utils.dates.days_ago(1)}&end_at={airflow.utils.dates.days_ago(0)}&symbols=EUR&base=GBP",
    gcs_bucket="airflow_training_bucket",
    task_id="fetch_exchange_rates",
    gcs_path="bucket",
    method="GET",
    http_conn_id="http_default",
    gcs_conn_id="google_cloud_default",)

fetch_exchange_rates
