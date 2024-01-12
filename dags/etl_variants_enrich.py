from airflow import DAG
from airflow.models.param import Param
from datetime import datetime
from lib import config
from lib.config import env, Env, K8sContext
from lib.operators.spark import SparkOperator

with DAG(
        dag_id='etl_variant_enrich',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        params={
            'project': Param('cqdg', type='string'),
        },
) as dag:
    def project() -> str:
        return '{{ params.project }}'

    def spark_config():
        return 'etl-task-xlarge' if env == Env.PROD else 'etl-task-large'


    variant_task_enrich_variants = SparkOperator(
        task_id='variant_task_variant_enrich_snv',
        name='etl-variant_task_variant_enrich_snv',
        k8s_context=K8sContext.DEFAULT,
        spark_jar=config.variant_task_jar,
        spark_class='bio.ferlab.etl.enriched.RunEnrichGenomic',
        spark_config=spark_config(),
        arguments=['variants',
                   '--config', f'config/{env}-{project()}.conf',
                   '--steps', 'default'],
    )

    variant_task_enrich_consequences = SparkOperator(
        task_id='variant_task_variant_enrich_consequences',
        name='etl-variant_task_variant_enrich_consequences',
        k8s_context=K8sContext.DEFAULT,
        spark_jar=config.variant_task_jar,
        spark_class='bio.ferlab.etl.enriched.RunEnrichGenomic',
        spark_config=spark_config(),
        arguments=['consequences',
                   '--config', f'config/{env}-{project()}.conf',
                   '--steps', 'default'],
    )

    variant_task_enrich_variants >> variant_task_enrich_consequences
