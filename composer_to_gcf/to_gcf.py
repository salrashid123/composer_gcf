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

from google.oauth2 import id_token
import google.auth.transport.requests

import requests

default_dag_args = {
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

class GcpOidcOperator(SimpleHttpOperator):
    def execute(self, context):
        http = HttpHook(self.method, http_conn_id=self.http_conn_id)

        self.log.info("Calling HTTP method")

        target_audience = 'https://echoapp-6w42z6vi3q-uc.a.run.app'
        request = google.auth.transport.requests.Request()
        idt = id_token.fetch_id_token(request, target_audience)
        self.headers = { 'Authorization' : "Bearer " + idt }
        response = http.run(self.endpoint,
                            self.data,
                            self.headers,
                            self.extra_options)
        if self.response_check:
            if not self.response_check(response):
                raise AirflowException("Response check returned False.")
        return response.json()

with models.DAG(
        'callgcf',
        schedule_interval=datetime.timedelta(minutes=30),
        default_args=default_dag_args) as dag:

    def greeting():
        import logging
        logging.info('Hello World!')

    hello_python = python_operator.PythonOperator(
        task_id='hello',
        python_callable=greeting)

    call_gcf1 = GcpOidcOperator(
        task_id='get_op1',
        method='GET',
        http_conn_id='my_gcf_conn',
        endpoint='/echo',
        headers={},
        response_check=lambda response: False if len(response.json()) == 0 else True,
        dag=dag,
    )

    # call_gcf2 = GcpOidcOperator(
    #     task_id='get_op2',
    #     method='POST',
    #     http_conn_id='my_gcf_conn',
    #     data = {"aaa": "bbb"},
    #     endpoint='/',
    #     headers={},
    #     response_check=lambda response: False if len(response.json()) == 0 else True,
    #     dag=dag,
    # )

    hello_python >> call_gcf1

