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
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime


def _get_execution_date(**kwargs):
    return kwargs["execution_date"]


args = {
    "owner": "airflow",
    "start_date": datetime(2019, 12, 1),
}

with DAG(
    dag_id='fifth_dag',
    default_args=args,
    schedule_interval='@daily'
) as dag:

    print_execution_date = PythonOperator(task_id='print_execution_date', python_callable=_get_execution_date)

    wait_5 = BashOperator(task_id='wait_5', bash_command='sleep 5')

    wait_1 = BashOperator(task_id='wait_1', bash_command='sleep 1')

    wait_10 = BashOperator(task_id='wait_10', bash_command='sleep 10')

    the_end = DummyOperator(task_id='the_end')


print_execution_date >> [wait_5, wait_1, wait_10] >> the_end




