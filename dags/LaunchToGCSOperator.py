import airflow
from airflow.models import BaseOperator
from airflow.utils import apply_defaults

from dags.launch_hook import LaunchHook


class LaunchToGCSOperator(BaseOperator):

    @apply_defaults
    def __init__(self, start_date, end_date, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._start_date = start_date
        self._end_date = end_date

    def execute(self, context):
        hook = LaunchHook("10/10/2000", "10/11/2000")
        return hook.get_launches()
