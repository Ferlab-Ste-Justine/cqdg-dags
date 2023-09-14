from airflow import DAG
from airflow.models.param import Param
from datetime import datetime
from lib import config
from lib.config import env, K8sContext
from lib.operators.spark import SparkOperator

with DAG(
        dag_id='index_task',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        params={
            'release_id': Param('7', type='string'),
            'study_ids': Param('ST0000017', type='string'),
            'project': Param('cqdg', type='string'),
            'es_port': Param('9200', type='string')
        },
) as dag:
    def release_id() -> str:
        return '{{ params.release_id }}'


    def study_ids() -> str:
        return '{{ params.study_ids }}'

    def project() -> str:
        return '{{ params.project }}'

    def es_port() -> str:
        return '{{ params.es_port }}'

    study_centric = SparkOperator(
        task_id='study_centric',
        name='etl-index-study-centric',
        k8s_context=K8sContext.DEFAULT,
        spark_jar=config.spark_index_jar,
        spark_class='bio.ferlab.fhir.etl.IndexTask',
        spark_config='etl-index-task',
        arguments=[release_id(), study_ids(), 'study_centric', env, project(), config.es_url, es_port()],
    )

    participant_centric = SparkOperator(
        task_id='participant_centric',
        name='etl-index-participant-centric',
        k8s_context=K8sContext.DEFAULT,
        spark_jar=config.spark_index_jar,
        spark_class='bio.ferlab.fhir.etl.IndexTask',
        spark_config='etl-index-task',
        arguments=[release_id(), study_ids(), 'participant_centric', env, project(), config.es_url, es_port()],
    )

    file_centric = SparkOperator(
        task_id='file_centric',
        name='etl-index-file-centric',
        k8s_context=K8sContext.DEFAULT,
        spark_jar=config.spark_index_jar,
        spark_class='bio.ferlab.fhir.etl.IndexTask',
        spark_config='etl-index-task',
        arguments=[release_id(), study_ids(), 'file_centric', env, project(), config.es_url, es_port()],
    )

    biospecimen_centric = SparkOperator(
        task_id='biospecimen_centric',
        name='etl-index-biospecimen-centric',
        k8s_context=K8sContext.DEFAULT,
        spark_jar=config.spark_index_jar,
        spark_class='bio.ferlab.fhir.etl.IndexTask',
        spark_config='etl-index-task',
        arguments=[release_id(), study_ids(), 'biospecimen_centric', env, project(), config.es_url, es_port()],
    )

    participant_centric >> study_centric >> file_centric >> biospecimen_centric
