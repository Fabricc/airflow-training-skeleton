import datetime
from unittest.mock import MagicMock

import pytest
from airflow import DAG
from airflow.models import Connection

from dags.launch_hook import LaunchHook
from dags.operators.launch_operator import LaunchToGcsOperator

pytest_plugins = ["helpers_namespace"]

@pytest.fixture
def test_dag():
    """Airflow DAG for testing."""
    return DAG("test_dag",
               default_args={"owner": "airflow", "start_date": datetime.datetime(2018, 1, 1)},
               schedule_interval=datetime.timedelta(days=1),)

@pytest.helpers.register
def run_task(task, dag):
        """Run an Airflow task."""
        dag.clear()
        task.run(start_date=dag.default_args["start_date"],
                 end_date=dag.default_args["start_date"],)


class TestOperator:
    def test_hook(self, test_dag, mocker, tmpdir):
        mocker.patch.object(LaunchHook, "getConnection", return_value=Connection())
        task = LaunchToGcsOperator(
            task_id="launch_to_gcs",
            start_date=datetime.datetime(2018, 1, 1),
            end_date=datetime.datetime(2018, 1, 2),
            output_bucket="output_bucket",
            output_path="output_bucket",
    )
        pytest.helpers.run_task(task=task, dag=test_dag)