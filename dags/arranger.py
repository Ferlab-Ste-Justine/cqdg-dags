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
        dag_id='update_arranger_project',
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
    def project_version() -> str:
        return '{{ params.project_version }}'



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
