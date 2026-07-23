from datetime import datetime

from airflow import DAG
from airflow.models import Variable, Param

from cqdg.lib.config import datalake_bucket, etl_base_config, spark_small_conf, obo_parser_jar


def obo_parser():
    return etl_base_config \
        .with_extra_env_variables(
        {'key': 'DATALAKE_BUCKET', 'value': 'datalake_bucket'},
        {'key': 'OBJECT_STORE_URL', 'value': 'object_store_url'}
        ) \
        .with_spark_class('bio.ferlab.HPOMain') \
        .with_image(Variable.get('obo_parser_image')) \
        .with_spark_jar(obo_parser_jar) \
        .add_spark_conf(spark_small_conf) \
        .args(
        '-f', obo_url(),
        '-t', ontology_type(),
        '-n', required_top_node()) \
        .operator(
        task_id='obo_parser_task',
        name='obo_parser-task'
    )

with DAG(
        dag_id='obo-parser',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        params={
            'obo_url': Param('https://purl.obolibrary.org/obo/hp.obo',
                             type='string'),
            'ontology_type': Param('hpo', type='string', enum=['icd-o', 'icd-10', 'mondo', 'hpo', 'ncit']),
            'required_top_node': Param("", type=["null", "string"]),
        },
) as dag:
    def obo_url() -> str:
        return '{{ params.obo_url }}'

    def ontology_type() -> str:
        return '{{ params.ontology_type }}'

    def required_top_node() -> str:
        return '{{ params.required_top_node }}'

    obo_parser()
