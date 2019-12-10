from airflow.hooks.base_hook import BaseHook
import requests


class LaunchHook(BaseHook):

    base_url = 'https://launchlibrary.net'

    def __init__(self, conn_id, api_version):
        super().__init__(source=None)
        self._conn_id = conn_id
        self._api_version = api_version

        self._conn = None

    def get_conn(self):
        session = requests.Session()
        return session

    def get_launches(self, start_date: str, end_date: str):
        session = self.get_conn()
        response = session.get(
            "{self.base_url}/{self.api_version}/launches",
            params={"start_date":start_date, "end_date": end_date}
        )

        response.raise_for_get_status()

        ## Can also e.g. add pagination

        return response.json()["launches"]





