from airflow import DAG
from airflow.models.param import Param
from datetime import datetime
from lib.config import env, es_url, Env, K8sContext
from lib.operators.fhavro import FhavroOperator

if env in [Env.QA, Env.DEV]:

    with DAG(
            dag_id='etl_fhavro_export',
            start_date=datetime(2022, 1, 1),
            schedule_interval=None,
            params={
                'color': Param('', enum=['', 'blue', 'green']),
            },
    ) as dag:

        fhavro_export = FhavroOperator(
            task_id='fhavro_export',
            name='etl-fhavro-export',
            k8s_context=K8sContext.DEFAULT,
            arguments=[
                '5', 'STU0000001', f'{env}',
            ],
        )
