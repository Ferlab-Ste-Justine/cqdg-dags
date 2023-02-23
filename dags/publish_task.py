from airflow import DAG
from airflow.models.param import Param
from airflow.utils.task_group import TaskGroup
from datetime import datetime
from lib import config
from lib.operators.fhavro import FhavroOperator
from lib.config import env, Env, K8sContext
from lib.operators.spark import SparkOperator
from lib.operators.arranger import ArrangerOperator
# if env in [Env.QA, Env.DEV]:

with DAG(
        dag_id='publish_task',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        params={
            'release_id': Param('7', type='string'),
            'study_ids': Param('ST0000017', type='string'),
            'job_type': Param('study_centric', type='string'),
            # 'env': Param('dev', type='string'),
            # 'env': Param('qa', enum=['dev', 'qa', 'prd']),
            'project': Param('cqdg', type='string'),
            'es_host': Param('http://elasticsearch-workers', type='string'),
            'es_port': Param('9200', type='string'),
        },
) as dag:

    def release_id() -> str:
        return '{{ params.release_id }}'

    def study_ids() -> str:
        return '{{ params.study_ids }}'

    def job_type() -> str:
        return '{{ params.job_type }}'

    def project() -> str:
        return '{{ params.project }}'

    def es_host() -> str:
        return '{{ params.es_host }}'

    def es_port() -> str:
        return '{{ params.es_port }}'

    with TaskGroup(group_id='publish') as publish:
        study_publish_task = SparkOperator(
            task_id='study_publish_task',
            name='etl-publish-study-task',
            k8s_context=K8sContext.DEFAULT,
            spark_jar=config.spark_publish_jar,
            spark_class='bio.ferlab.fhir.etl.PublishTask',
            spark_config='etl-index-task',
            arguments=[es_host(), es_port(), env, project(), release_id(), study_ids(), 'study_centric'],
        )

        participant_publish_task = SparkOperator(
            task_id='participant_publish_task',
            name='etl-publish-participant-task',
            k8s_context=K8sContext.DEFAULT,
            spark_jar=config.spark_publish_jar,
            spark_class='bio.ferlab.fhir.etl.PublishTask',
            spark_config='etl-index-task',
            arguments=[es_host(), es_port(), env, project(), release_id(), study_ids(), 'participant_centric'],
        )

        file_publish_task = SparkOperator(
            task_id='file_publish_task',
            name='etl-publish-file-task',
            k8s_context=K8sContext.DEFAULT,
            spark_jar=config.spark_publish_jar,
            spark_class='bio.ferlab.fhir.etl.PublishTask',
            spark_config='etl-index-task',
            arguments=[es_host(), es_port(), env, project(), release_id(), study_ids(), 'file_centric'],
        )

        biospecimen_publish_task = SparkOperator(
            task_id='biospecimen_publish_task',
            name='etl-publish-biospecimen-task',
            k8s_context=K8sContext.DEFAULT,
            spark_jar=config.spark_publish_jar,
            spark_class='bio.ferlab.fhir.etl.PublishTask',
            spark_config='etl-index-task',
            arguments=[es_host(), es_port(), env, project(), release_id(), study_ids(), 'biospecimen_centric'],
        )

        study_publish_task >> participant_publish_task >> file_publish_task >> biospecimen_publish_task
