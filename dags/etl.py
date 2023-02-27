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
        dag_id='etl',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        params={
            'release_id': Param('7', type='string'),
            'study_ids': Param('ST0000017', type='string'),
            'job_type': Param('study_centric', type='string'),
            'project': Param('cqdg', type='string'),
            'es_host': Param('http://elasticsearch-workers', type='string'),
            'es_port': Param('9200', type='string'),
            'project_version': Param('v1', type='string'),
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

    def project_version() -> str:
        return '{{ params.project_version }}'

    # fhavro_export_task = FhavroOperator(
    #     task_id='fhavro-import-task',
    #     name='etl-fhavro-import-task',
    #     k8s_context=K8sContext.ETL,
    #     spark_jar=config.spark_index_jar,
    #     spark_class='bio.ferlab.fhir.etl.FhavroExport',
    #     spark_config='enriched-etl',
    #     arguments=['7', 'ST0000017', 'dev'],
    # )
    #
    # import_task = SparkOperator(
    #     task_id='import-task',
    #     name='etl-import-task',
    #     k8s_context=K8sContext.ETL,
    #     spark_jar=config.spark_index_jar,
    #     spark_class='bio.ferlab.fhir.etl.ImportTask',
    #     spark_config='enriched-etl',
    #     arguments=['./config/dev-cqdg.conf', 'default', '5', 'STU0000001'],
    # )
    #
    # prepare_index_task = SparkOperator(
    #     task_id='prepare-index-task',
    #     name='etl-prepare-index-task',
    #     k8s_context=K8sContext.ETL,
    #     spark_jar=config.spark_index_jar,
    #     spark_class='bio.ferlab.fhir.etl.PrepareIndex',
    #     spark_config='enriched-etl',
    #     arguments=['./config/dev-cqdg.conf', 'default', 'participant_centric', '5', 'STU0000001'],
    #1 )
    with TaskGroup(group_id='index') as index:
        study_centric = SparkOperator(
            task_id='study_centric',
            name='etl-index-study-centric',
            k8s_context=K8sContext.DEFAULT,
            spark_jar=config.spark_index_jar,
            spark_class='bio.ferlab.fhir.etl.IndexTask',
            spark_config='etl-index-task',
            arguments=[release_id(), study_ids(), 'study_centric', env, project(), es_host(), es_port()],
        )

        participant_centric = SparkOperator(
            task_id='participant_centric',
            name='etl-index-participant-centric',
            k8s_context=K8sContext.DEFAULT,
            spark_jar=config.spark_index_jar,
            spark_class='bio.ferlab.fhir.etl.IndexTask',
            spark_config='etl-index-task',
            arguments=[release_id(), study_ids(), 'participant_centric', env, project(), es_host(), es_port()],
        )

        file_centric = SparkOperator(
            task_id='file_centric',
            name='etl-index-file-centric',
            k8s_context=K8sContext.DEFAULT,
            spark_jar=config.spark_index_jar,
            spark_class='bio.ferlab.fhir.etl.IndexTask',
            spark_config='etl-index-task',
            arguments=[release_id(), study_ids(), 'file_centric', env, project(), es_host(), es_port()],
        )

        biospecimen_centric = SparkOperator(
            task_id='biospecimen_centric',
            name='etl-index-biospecimen-centric',
            k8s_context=K8sContext.DEFAULT,
            spark_jar=config.spark_index_jar,
            spark_class='bio.ferlab.fhir.etl.IndexTask',
            spark_config='etl-index-task',
            arguments=[release_id(), study_ids(), 'biospecimen_centric', env, project(), es_host(), es_port()],
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
            arguments=[es_host(), es_port(), env, project(), release_id(), study_ids(), 'study_centric'],
        )

        participant_centric = SparkOperator(
            task_id='participant_centric',
            name='etl-publish-participant-centric',
            k8s_context=K8sContext.DEFAULT,
            spark_jar=config.spark_publish_jar,
            spark_class='bio.ferlab.fhir.etl.PublishTask',
            spark_config='etl-index-task',
            arguments=[es_host(), es_port(), env, project(), release_id(), study_ids(), 'participant_centric'],
        )

        file_centric = SparkOperator(
            task_id='file_centric',
            name='etl-publish-file-centric',
            k8s_context=K8sContext.DEFAULT,
            spark_jar=config.spark_publish_jar,
            spark_class='bio.ferlab.fhir.etl.PublishTask',
            spark_config='etl-index-task',
            arguments=[es_host(), es_port(), env, project(), release_id(), study_ids(), 'file_centric'],
        )

        biospecimen_centric = SparkOperator(
            task_id='biospecimen_centric',
            name='etl-publish-biospecimen-centric',
            k8s_context=K8sContext.DEFAULT,
            spark_jar=config.spark_publish_jar,
            spark_class='bio.ferlab.fhir.etl.PublishTask',
            spark_config='etl-index-task',
            arguments=[es_host(), es_port(), env, project(), release_id(), study_ids(), 'biospecimen_centric'],
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

