from datetime import datetime

from airflow import DAG
from airflow.models import Variable, Param

from cqdg.lib.config import datalake_bucket, etl_base_config, spark_small_conf, obo_parser_jar


def obo_parser():
    return etl_base_config \
        .with_spark_class('bio.ferlab.HPOMain') \
        .with_image(Variable.get('obo_parser_image')) \
        .with_spark_jar(obo_parser_jar) \
        .add_spark_conf(spark_small_conf) \
        .args(obo_url(), datalake_bucket, ontology(), is_icd(), required_top_node()) \
        .operator(
        task_id='obo_parser_task',
        name='obo_parser-task'
    )

with DAG(
        dag_id='obo-parser',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        params={
            'obo_url': Param('https://raw.githubusercontent.com/obophenotype/human-phenotype-ontology/master/hp.obo',
                             type='string'),
            'ontology': Param('hpo_terms', type='string'),
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

    obo_parser()
