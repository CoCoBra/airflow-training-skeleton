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
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from datetime import datetime


def _get_execution_date(**context):
    print(context['execution_date'])


def _get_branch(**context):
    week_day = context['execution_date'].weekday()
    if week_day in [0, 1]:
        return "email_bob"
    if week_day in [2, 3, 4]:
        return "email_alice"
    else:
        return "email_joe"


args = {
    "owner": "airflow",
    "start_date": datetime(2019, 12, 1),
}

with DAG(
    dag_id='sixth_dag',
    default_args=args,
    schedule_interval='@daily'
) as dag:

    print_execution_date = PythonOperator(task_id='print_execution_date',
                                          python_callable=_get_execution_date,
                                          provide_context=True)

    branching = BranchPythonOperator(task_id='branching',
                                     python_callable=_get_branch,
                                     provide_context=True)

    email_bob = DummyOperator(task_id='email_bob')
    email_alice = DummyOperator(task_id='email_alice')
    email_joe = DummyOperator(task_id='email_joe')

    final_task = BashOperator(task_id='final_task', bash_command="echo final task", trigger_rule="one_success")


print_execution_date >> branching >> [email_bob, email_alice, email_joe] >> final_task




