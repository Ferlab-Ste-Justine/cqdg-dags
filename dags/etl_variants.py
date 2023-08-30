from airflow import DAG
from airflow.models.param import Param
from datetime import datetime
from lib import config
from lib.config import env, Env, K8sContext
from lib.operators.spark import SparkOperator

with DAG(
        dag_id='etl_variant',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        params={
            'study_id': Param('ST0000017', type='string'),
            'release_id': Param('1', type='string'),
            'project': Param('cqdg', type='string'),
        },
) as dag:

    def study_id() -> str:
        return '{{ params.study_id }}'

    def project() -> str:
        return '{{ params.project }}'

    def release_id() -> str:
        return '{{ params.release_id }}'


    variant_task_snv = SparkOperator(
        task_id='variant-task_snv',
        name='etl-variant-task_snv',
        k8s_context=K8sContext.DEFAULT,
        spark_jar=config.variant_task_jar,
        spark_class='bio.ferlab.etl.normalized.RunNormalizedGenomic',
        spark_config='etl-index-task',
        arguments=['snv',
                   '--config', f'config/{env}-{project()}.conf',
                   '--steps', 'default',
                   '--app-name', 'variant_task_consequences',
                   '--release-id', release_id(),
                   '--study-id', study_id(),
                   '--vcf-pattern', ''],
    )

    variant_task_consequences = SparkOperator(
        task_id='variant_task_consequences',
        name='etl-variant_task_consequences',
        k8s_context=K8sContext.DEFAULT,
        spark_jar=config.variant_task_jar,
        spark_class='bio.ferlab.etl.normalized.RunNormalizedGenomic',
        spark_config='etl-index-task',
        arguments=['consequences',
                   '--config', f'config/{env}-{project()}.conf',
                   '--steps', 'default',
                   '--app-name', 'variant_task_consequences',
                   '--study-id', study_id(),
                   '--vcf-pattern', ''],
    )


variant_task_snv >> variant_task_consequences
