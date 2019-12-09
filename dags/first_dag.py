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

import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

args = {
    "owner": "airflow",
    "start_date": airflow.utils.dates.days_ago(2),
}

dag = DAG(
    dag_id='first_dag',
    default_args=args,
    schedule_interval='0 0 * * *',
    dagrun_timeout=timedelta(minutes=60),
)

t1 = DummyOperator(
    task_id='t1',
    dag=dag,
)

t2 = DummyOperator(
    task_id='t2',
    dag=dag,
)

t1 >> t2


t3 = DummyOperator(
    task_id='t3',
    dag=dag,
)

t4 = DummyOperator(
    task_id='t4',
    dag=dag,
)

t2 >> t3
t2 >> t4

t5 = DummyOperator(
    task_id='t5',
    dag=dag,
)

(t3, t4) >> t5
