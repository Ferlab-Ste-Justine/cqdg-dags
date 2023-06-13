from airflow import DAG
from airflow.models.param import Param
from datetime import datetime
from lib import config
from lib.config import env, Env, K8sContext
from lib.operators.spark import SparkOperator

with DAG(
        dag_id='etl_enrich_specimen',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        params={
            'study_ids': Param('ST0000042', type='string'),
            'project': Param('cqdg', type='string'),
        },
) as dag:

    def study_ids() -> str:
        return '{{ params.study_ids }}'

    def project() -> str:
        return '{{ params.project }}'


    enrich_specimen = SparkOperator(
        task_id='enrich-specimen',
        name='etl-enrich-specimen',
        k8s_context=K8sContext.DEFAULT,
        spark_jar=config.spark_prepare_index_jar,
        spark_class='bio.ferlab.fhir.etl.Enrich',
        spark_config='etl-index-task',
        arguments=[f'config/{env}-{project()}.conf', 'default', 'all', study_ids()],
    )

