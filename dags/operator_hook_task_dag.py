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


from datetime import timedelta
from airflow.models import DAG
from airflow.contrib.operators.postgres_to_gcs_operator import PostgresToGoogleCloudStorageOperator
from datetime import datetime

get_postgres_data_query = """
    SELECT * 
    FROM land_registry_price_paid_uk
    WHERE transfer_date = '{{ execution_date.date() }}';
"""

args = {
    "owner": "airflow",
    "start_date": datetime(2019, 12, 5),
}

with DAG(
    dag_id='operator_hook_task_dag',
    default_args=args,
    schedule_interval='@daily'
) as dag:

    postgres_to_gcp = PostgresToGoogleCloudStorageOperator(
        task_id='postgres_to_gcp',
        postgres_conn_id='postgres_training',
        sql=get_postgres_data_query,
        bucket="airflow_training_ccb",
        export_format="csv",
        filename="land_registry_price_paid_uk/{{ execution_date.date() }}.csv"
    )




