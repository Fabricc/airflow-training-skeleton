import pathlib
import posixpath
import json
import requests

import airflow
from airflow.hooks.base_hook import BaseHook


class LaunchHook(BaseHook):
    def __init__(self, first_date, end_date):
        super().__init__()
        self._first_date = first_date
        self._end_date = end_date

    def _print_stats(self):
        with open(f"/tmp/rocket_launches/ds={self.first_date}/launches.json") as f:
            data = json.load(f)
            rockets_launched = [launch["name"] for launch in data["launches"]]
            rockets_str = ""
            if rockets_launched:
                rockets_str = f" ({' & '.join(rockets_launched)})"
                print(f"{len(rockets_launched)} rocket launch(es) on {self.first_date}{rockets_str}.")

    def _download_rocket_launches(self):
        query = f"https://launchlibrary.net/1.4/launch?startdate={self.first_date}&enddate={self.end_date}"
        result_path = f"/tmp/rocket_launches/ds={self.first_date}"
        pathlib.Path(result_path).mkdir(parents=True, exist_ok=True)
        response = requests.get(query)
        with open(posixpath.join(result_path, "launches.json"), "w") as f:
            f.write(response.text)

        return self._print_stats(self)

    def get_launches(self, **kwargs):
        return self._download_rocket_launches(self)
