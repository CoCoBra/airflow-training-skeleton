# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


from airflow.models import DAG
from datetime import datetime

from airflow.hooks.base_hook import BaseHook
import requests

from airflow.models import BaseOperator
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.utils.decorators import apply_defaults
import tempfile
import os
import json


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

class LaunchToGcsOperator(BaseOperator):

    ui_color = '#555’'

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


args = {
    "owner": "airflow",
    "start_date": datetime(2019, 12, 5),
}

with DAG(
    dag_id='build_own_operator_dag',
    default_args=args,
    schedule_interval='@daily'
) as dag:

    launch_to_gcp = LaunchToGcsOperator(
        task_id='launch_to_gcp',
        output_bucket="airflow_training_ccb",
        output_path="launch_data/{{ ds }}.json",
        start_date="{{ ds }}",
        end_date="{{ tomorrow_ds }}"

    )

    # print_stats = PythonOperator(
    #
    #
    # )



