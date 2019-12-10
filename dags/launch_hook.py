import pathlib
import posixpath
import json
import requests

import airflow
from airflow.models.hooks import BaseHook


class LaunchHook(BaseHook):
    def __init__(self, start_date, end_date):
        super().__init__()
        self._start_date = start_date
        self._end_date = end_date

    def _print_stats(self):
        with open(f"/data/rocket_launches/ds={start_date}/launches.json") as f:
            data = json.load(f)
            rockets_launched = [launch["name"] for launch in data["launches"]]
            rockets_str = ""
            if rockets_launched:
                rockets_str = f" ({' & '.join(rockets_launched)})"
                print(f"{len(rockets_launched)} rocket launch(es) on {start_date}{rockets_str}.")

    def _download_rocket_launches(self):
        query = f"https://launchlibrary.net/1.4/launch?startdate={self.start_date}&enddate={self.end_date}"
        result_path = f"/data/rocket_launches/ds={self.start_date}"
        pathlib.Path(result_path).mkdir(parents=True, exist_ok=True)
        response = requests.get(query)
        with open(posixpath.join(result_path, "launches.json"), "w") as f:
            f.write(response.text)

        return self._print_stats(self.start_date)

    def get_launches(self, **kwargs):
        return self._download_rocket_launches(self)
    m
