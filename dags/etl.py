from airflow import DAG
from airflow.models.param import Param
from datetime import datetime
from lib import config
from lib.config import env, Env, K8sContext
from lib.operators.spark import SparkOperator

# if env in [Env.QA, Env.DEV]:

with DAG(
        dag_id='etl',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        params={
            'release_id': Param('5', type='string'),
            'study_ids': Param('STU0000001', type='string'),
            'jobType': Param('participant_centric', type='string'),
            'env': Param('', enum=['dev', 'qa', 'prd']),
            'project': Param('cqdg', type='string'),
        },
) as dag:


    index_task = SparkOperator(
        task_id='index-task',
        name='etl-index-task',
        k8s_context=K8sContext.ETL,
        spark_jar=config.spark_index_jar,
        spark_class='bio.ferlab.fhir.etl.IndexTask',
        spark_config='enriched-etl',
        arguments=['./config/dev-cqdg.conf', 'default', '5', 'STU0000001'],
    )
