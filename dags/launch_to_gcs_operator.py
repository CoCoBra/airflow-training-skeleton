from airflow.models import BaseOperator
from launch_hook import LaunchHook
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.utils.decorators import apply_defaults
import tempfile
import os
import json
from launch_hook import LaunchHook


class LaunchToGcsOperator(BaseOperator):

    ui_color = '#555’'
    ui_fgcolor = '#fff'

    @apply_defaults
    def __init__(self, start_date, end_date, output_bucket, output_path, **kwargs):
        super().__init__( **kwargs)

        # self._gcp_conn_id = gcp_conn_id

        self.start_date = start_date
        self.end_date = end_date

        self.output_bucket = output_bucket
        self.output_path = output_path

    def execute(self, context):

        results = self._query_launch_api()

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = os.path.join(tmp_dir, 'results.json')
            with open(tmp_path, "w") as file_:
                json.dump(results, file_)

        self._upload_to_gcs_bucket(tmp_path)

    def _query_launch_api(self):
        lh = LaunchHook()
        launch_data = lh.get_launches(
            start_date=self.start_date,
            end_date=self.end_date
        )

        return launch_data

    def _upload_to_gcs_bucket(self, tmp_path):

        gcs_hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=self._gcp_conn_id
        )

        gcs_hook.upload(
            bucket=self.output_bucket,
            object=self.output_path,
            filename=tmp_path
        )



