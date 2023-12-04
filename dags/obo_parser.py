from airflow import DAG
from airflow.models.param import Param
from datetime import datetime
from lib import config
from lib.config import env, Env, K8sContext
from lib.operators.spark import SparkOperator

with DAG(
        dag_id='obo_parser',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        params={
            'obo_url': Param('https://raw.githubusercontent.com/obophenotype/human-phenotype-ontology/master/hp.obo', type='string'),
            'ontology ': Param("hpo_terms", type='string'),
            'is_icd': Param(False, type='boolean'),
            'required_top_node': Param("", type='string'),
        },
) as dag:
    def obo_url() -> str:
        return '{{ params.obo_url }}'

    def ontology() -> str:
        return '{{ params.ontology }}'
    def is_icd() -> str:
        return '{{ params.is_icd }}'

    def required_top_node() -> str:
        return '{{ params.required_top_node }}'

    import_task = SparkOperator(
        task_id='obo_parser_task',
        name='obo_parser-task',
        k8s_context=K8sContext.DEFAULT,
        spark_jar=config.obo_parser_jar,
        spark_class='bio.ferlab.HPOMain',
        spark_config='etl-task-small',
        arguments=[obo_url(), f'cqdg-{env}-app-datalake', ontology(), is_icd(), required_top_node()],
    )
