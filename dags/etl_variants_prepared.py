from airflow import DAG
from airflow.models.param import Param
from datetime import datetime
from lib import config
from lib.config import env, Env, K8sContext
from lib.operators.spark import SparkOperator

with DAG(
        dag_id='etl_variant_prepared',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        params={
            'project': Param('cqdg', type='string'),
        },
) as dag:

    def project() -> str:
        return '{{ params.project }}'

    variant_task_variant_centric = SparkOperator(
        task_id='variant_task_variant_centric',
        name='etl-variant_task_variant_centric',
        k8s_context=K8sContext.DEFAULT,
        spark_jar=config.variant_task_jar,
        spark_class='bio.ferlab.etl.prepared.RunPrepared',
        spark_config='etl-task-medium',
        arguments=['variant_centric',
                   '--config', f'config/{env}-{project()}.conf',
                   '--steps', 'default'],
    )

    variant_task_gene_centric = SparkOperator(
        task_id='variant_task_gene_centric',
        name='etl-variant_task_gene_centric',
        k8s_context=K8sContext.DEFAULT,
        spark_jar=config.variant_task_jar,
        spark_class='bio.ferlab.etl.prepared.RunPrepared',
        spark_config='etl-task-medium',
        arguments=['gene_centric',
                   '--config', f'config/{env}-{project()}.conf',
                   '--steps', 'default'],
    )

    variant_task_variant_suggestions = SparkOperator(
        task_id='variant_task_variant_suggestions',
        name='etl-variant_variant_suggestions',
        k8s_context=K8sContext.DEFAULT,
        spark_jar=config.variant_task_jar,
        spark_class='bio.ferlab.etl.prepared.RunPrepared',
        spark_config='etl-task-medium',
        arguments=['variant_suggestions',
                   '--config', f'config/{env}-{project()}.conf',
                   '--steps', 'default'],
    )

    variant_task_gene_suggestions = SparkOperator(
        task_id='variant_task_gene_suggestions',
        name='etl-variant_gene_suggestions',
        k8s_context=K8sContext.DEFAULT,
        spark_jar=config.variant_task_jar,
        spark_class='bio.ferlab.etl.prepared.RunPrepared',
        spark_config='etl-task-medium',
        arguments=['gene_suggestions',
                   '--config', f'config/{env}-{project()}.conf',
                   '--steps', 'default'],
    )

    variant_task_variant_centric >> variant_task_gene_centric >> variant_task_variant_suggestions >> variant_task_gene_suggestions
