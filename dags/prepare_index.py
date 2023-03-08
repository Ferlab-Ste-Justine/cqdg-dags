from airflow import DAG
from airflow.models.param import Param
from datetime import datetime
from lib import config
from lib.config import env, K8sContext
from lib.operators.spark import SparkOperator

with DAG(
        dag_id='prepare_index',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        params={
            'release_id': Param('7', type='string'),
            'study_ids': Param('ST0000017', type='string'),
            'project': Param('cqdg', type='string'),
        },
) as dag:
    def release_id() -> str:
        return '{{ params.release_id }}'


    def study_ids() -> str:
        return '{{ params.study_ids }}'

    def project() -> str:
        return '{{ params.project }}'

    prepare_index = SparkOperator(
        task_id='prepare_index',
        name='etl-prepare-index',
        k8s_context=K8sContext.DEFAULT,
        spark_jar=config.spark_prepare_index_jar,
        spark_class='bio.ferlab.fhir.etl.PrepareIndex',
        spark_config='etl-index-task',
        arguments=[f'config/{env}-{project()}.conf', 'default', 'all', release_id(), study_ids()],
    )
