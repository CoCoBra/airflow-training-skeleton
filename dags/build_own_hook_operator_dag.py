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
from launch_to_gcs_operator import LaunchToGcsOperator
import tempfile

args = {
    "owner": "airflow",
    "start_date": datetime(2019, 12, 5),
}

# def _print_stats():


with DAG(
    dag_id='build_own_operator_dag',
    default_args=args,
    schedule_interval='@daily'
) as dag:

    launch_to_gcp = LaunchToGcsOperator(
        task_id='launch_to_gcp',
        bucket_name="airflow_training_ccb",
        output_path="launch_data/{{ ds }}.json",
        start_date="{{ ds }}",
        end_date="{{ tomorrow_ds }}"

    )

    # print_stats = PythonOperator(
    #
    #
    # )



