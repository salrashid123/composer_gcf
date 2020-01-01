import datetime
import os

from airflow import models
from airflow.utils import trigger_rule

import json
from datetime import timedelta

import airflow
from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators import bash_operator
from airflow.operators import python_operator
from airflow.exceptions import AirflowException
from airflow.hooks.http_hook import HttpHook

import requests


gcf_dag_args = {
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'retries': 1,
}


with models.DAG(
        'fromgcf',
        schedule_interval=None,
        default_args=gcf_dag_args) as dag:

    def greeting():
        import logging
        logging.info('Hello from GCF!')

    from_gcf_invoke = python_operator.PythonOperator(
        task_id='hello_from_gcf',
        python_callable=greeting,
        dag=dag)
    from_gcf_invoke


