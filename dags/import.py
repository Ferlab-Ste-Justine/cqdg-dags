from airflow import DAG
from airflow.models.param import Param
from datetime import datetime
from lib import config
from lib.config import env, Env, K8sContext
from lib.operators.spark import SparkOperator

with DAG(
        dag_id='import',
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

    import_task = SparkOperator(
        task_id='import_task',
        name='etl-import-task',
        k8s_context=K8sContext.DEFAULT,
        spark_jar=config.spark_import_jar,
        spark_class='bio.ferlab.fhir.etl.ImportTask',
        spark_config='etl-task-medium',
        arguments=[f'config/{env}-{project()}.conf', 'default', release_id(), study_ids()],
    )
