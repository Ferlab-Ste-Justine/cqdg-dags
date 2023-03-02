from airflow import DAG
from airflow.models.param import Param
from airflow.utils.task_group import TaskGroup
from datetime import datetime
from lib import config
from lib.operators.fhavro import FhavroOperator
from lib.config import env, Env, K8sContext
from lib.operators.spark import SparkOperator
from lib.operators.arranger import ArrangerOperator

# 7

with DAG(
        dag_id='etl',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        params={
            'release_id': Param('7', type='string'),
            'study_ids': Param('ST0000017', type='string'),
            'project': Param('cqdg', type='string'),
            'es_port': Param('9200', type='string'),
            'project_version': Param('v1', type='string'),
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


    def project_version() -> str:
        return '{{ params.project_version }}'


    with TaskGroup(group_id='index') as index:
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
        study_centric >> participant_centric >> file_centric >> biospecimen_centric

    with TaskGroup(group_id='publish') as publish:
        study_centric = SparkOperator(
            task_id='study_centric',
            name='etl-publish-study-centric',
            k8s_context=K8sContext.DEFAULT,
            spark_jar=config.spark_publish_jar,
            spark_class='bio.ferlab.fhir.etl.PublishTask',
            spark_config='etl-index-task',
            arguments=[config.es_url, es_port(), env, project(), release_id(), study_ids(), 'study_centric'],
        )

        participant_centric = SparkOperator(
            task_id='participant_centric',
            name='etl-publish-participant-centric',
            k8s_context=K8sContext.DEFAULT,
            spark_jar=config.spark_publish_jar,
            spark_class='bio.ferlab.fhir.etl.PublishTask',
            spark_config='etl-index-task',
            arguments=[config.es_url, es_port(), env, project(), release_id(), study_ids(), 'participant_centric'],
        )

        file_centric = SparkOperator(
            task_id='file_centric',
            name='etl-publish-file-centric',
            k8s_context=K8sContext.DEFAULT,
            spark_jar=config.spark_publish_jar,
            spark_class='bio.ferlab.fhir.etl.PublishTask',
            spark_config='etl-index-task',
            arguments=[config.es_url, es_port(), env, project(), release_id(), study_ids(), 'file_centric'],
        )

        biospecimen_centric = SparkOperator(
            task_id='biospecimen_centric',
            name='etl-publish-biospecimen-centric',
            k8s_context=K8sContext.DEFAULT,
            spark_jar=config.spark_publish_jar,
            spark_class='bio.ferlab.fhir.etl.PublishTask',
            spark_config='etl-index-task',
            arguments=[config.es_url, es_port(), env, project(), release_id(), study_ids(), 'biospecimen_centric'],
        )

        study_centric >> participant_centric >> file_centric >> biospecimen_centric

    arranger_update_project = ArrangerOperator(
        task_id='arranger_update_project',
        name='etl-publish-arranger-update-project',
        k8s_context=K8sContext.DEFAULT,
        cmds=['node',
              '--experimental-modules=node',
              '--es-module-specifier-resolution=node',
              'admin/run.mjs',
              project_version(),
              ],
    )

    index >> publish >> arranger_update_project
