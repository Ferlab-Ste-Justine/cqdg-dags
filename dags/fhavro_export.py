from airflow import DAG
from airflow.models.param import Param
from airflow.utils.task_group import TaskGroup
from datetime import datetime
from lib import config
from lib.operators.fhavro import FhavroOperator
from lib.config import env, Env, K8sContext
from lib.operators.spark import SparkOperator
from lib.operators.arranger import ArrangerOperator

#11

with DAG(
        dag_id='fhavro_export',
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
