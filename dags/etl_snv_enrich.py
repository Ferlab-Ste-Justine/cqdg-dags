from airflow import DAG
from airflow.models.param import Param
from datetime import datetime
from lib import config
from lib.config import env, Env, K8sContext
from lib.operators.spark import SparkOperator

with DAG(
        dag_id='etl_snv_enrich',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        params={
            'study_id': Param('ST0000002', type='string'),
            'dataset': Param('dataset_default', type='string'),
            'batch': Param('annotated_vcf', type='string'),
            'project': Param('cqdg', type='string'),
        },
) as dag:
    def project() -> str:
        return '{{ params.project }}'

    def study_id() -> str:
        return '{{ params.study_id }}'

    def dataset() -> str:
        return '{{ params.dataset }}'
    
    def batch() -> str:
        return '{{ params.batch }}'    

    variant_task_enrich_snv = SparkOperator(
        task_id='variant_task_variant_enrich_snv',
        name='etl-variant_task_variant_enrich_snv',
        k8s_context=K8sContext.DEFAULT,
        spark_jar=config.variant_task_jar,
        spark_class='bio.ferlab.etl.enriched.RunEnrichGenomic',
        spark_config='etl-task-xlarge',
        arguments=['snv',
                   '--config', f'config/{env}-{project()}.conf',
                   '--steps', 'default',
                   '--app-name', 'variant_task_enrich_snv',
                   '--study-id', study_id(),
                   '--dataset', dataset(),
                   '--batch', batch()],                   
    )

    variant_task_enrich_snv
