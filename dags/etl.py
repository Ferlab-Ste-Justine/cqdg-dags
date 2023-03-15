from airflow import DAG
from airflow.models.param import Param
from airflow.utils.task_group import TaskGroup
from datetime import datetime
from lib import config
from lib.operators.fhavro import FhavroOperator
from lib.config import env, Env, K8sContext
from lib.operators.spark import SparkOperator
from lib.operators.arranger import ArrangerOperator

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
            'test': Param('false', type='string'),
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

    def test() -> str:
        return '{{ params.test }}'

    fhavro_export = FhavroOperator(
        task_id='fhavro_export',
        name='etl-fhavro_export',
        k8s_context=K8sContext.DEFAULT,
        cmds=['java',
              '-cp',
              'fhavro-export.jar',
              'bio/ferlab/fhir/etl/FhavroExport',
              release_id(), study_ids(), env
              ],
    )

    import_task = SparkOperator(
        task_id='import_task',
        name='etl-import-task',
        k8s_context=K8sContext.DEFAULT,
        spark_jar=config.spark_import_jar,
        spark_class='bio.ferlab.fhir.etl.ImportTask',
        spark_config='etl-index-task',
        arguments=[f'config/{env}-{project()}.conf', 'default', release_id(), study_ids()],
    )

    prepare_index = SparkOperator(
        task_id='prepare_index',
        name='etl-prepare-index',
        k8s_context=K8sContext.DEFAULT,
        spark_jar=config.spark_prepare_index_jar,
        spark_class='bio.ferlab.fhir.etl.PrepareIndex',
        spark_config='etl-index-task',
        arguments=[f'config/{env}-{project()}.conf', 'default', 'all', release_id(), study_ids()],
    )

    with TaskGroup(group_id='index') as index:
        study_centric = SparkOperator(
            task_id='study_centric',
            name='etl-index-study-centric',
            k8s_context=K8sContext.DEFAULT,
            spark_jar=config.spark_index_jar,
            spark_class='bio.ferlab.fhir.etl.IndexTask',
            spark_config='etl-index-task',
            arguments=[release_id(), study_ids(), 'study_centric', env, project(), config.es_url, es_port(), test()],
        )

        participant_centric = SparkOperator(
            task_id='participant_centric',
            name='etl-index-participant-centric',
            k8s_context=K8sContext.DEFAULT,
            spark_jar=config.spark_index_jar,
            spark_class='bio.ferlab.fhir.etl.IndexTask',
            spark_config='etl-index-task',
            arguments=[release_id(), study_ids(), 'participant_centric', env, project(), config.es_url, es_port(), test()],
        )

        file_centric = SparkOperator(
            task_id='file_centric',
            name='etl-index-file-centric',
            k8s_context=K8sContext.DEFAULT,
            spark_jar=config.spark_index_jar,
            spark_class='bio.ferlab.fhir.etl.IndexTask',
            spark_config='etl-index-task',
            arguments=[release_id(), study_ids(), 'file_centric', env, project(), config.es_url, es_port(), test()],
        )

        biospecimen_centric = SparkOperator(
            task_id='biospecimen_centric',
            name='etl-index-biospecimen-centric',
            k8s_context=K8sContext.DEFAULT,
            spark_jar=config.spark_index_jar,
            spark_class='bio.ferlab.fhir.etl.IndexTask',
            spark_config='etl-index-task',
            arguments=[release_id(), study_ids(), 'biospecimen_centric', env, project(), config.es_url, es_port(), test()],
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

    fhavro_export >> import_task >> prepare_index >> index >> publish >> arranger_update_project
