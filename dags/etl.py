from airflow import DAG
from airflow.models.param import Param

from datetime import datetime

from lib import config
from lib.operators.fhavro import FhavroOperator
from lib.config import env, Env, K8sContext
from lib.operators.spark import SparkOperator
# if env in [Env.QA, Env.DEV]:

with DAG(
        dag_id='etl',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        params={
            'release_id': Param('7', type='string'),
            'study_ids': Param('ST0000017', type='string'),
            'job_type': Param('study_centric', type='string'),
            'env': Param('qa', type='string'),
            # 'env': Param('qa', enum=['dev', 'qa', 'prd']),
            'project': Param('cqdg', type='string'),
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

    def _env() -> str:
        return '{{ params.env }}'

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
    # )

    index_task = SparkOperator(
        task_id='index-task',
        name='etl-index-task',
        k8s_context=K8sContext.DEFAULT,
        spark_jar=config.spark_index_jar,
        spark_class='bio.ferlab.fhir.etl.IndexTask',
        spark_config='etl-index-task',
        arguments=[release_id(), study_ids(), job_type(), _env(), project(), 'https://elasticsearch-workers', '9200'],
    )
