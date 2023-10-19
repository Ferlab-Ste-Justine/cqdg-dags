from airflow import DAG
from airflow.models.param import Param
from airflow.utils.task_group import TaskGroup
from datetime import datetime
from lib import config
from lib.config import env, K8sContext
from lib.operators.spark import SparkOperator

with DAG(
        dag_id='variant-index_task',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        params={
            'release_id': Param('7', type='string'),
            'study_ids': Param('ST0000017', type='string'),
            'project': Param('cqdg', type='string'),
            'es_port': Param('9200', type='string')
        },
) as dag:
    def release_id() -> str:
        return '{{ params.release_id }}'

    def study_ids() -> str:
        return '{{ params.study_ids }}'


    def project() -> str:
        return '{{ params.project }}'


    def es_port() -> str:
        return '{{ params.es_port }}'


    chromosomes = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18',
                   '19', '20', '21', '22', 'X', 'Y', 'M']

    def genes():
        gene_centric_task = SparkOperator(
            task_id=f'gene_centric_index',
            name=f'etl-index-gene-centric-index',
            k8s_context=K8sContext.DEFAULT,
            spark_jar=config.spark_index_jar,
            spark_class='bio.ferlab.fhir.etl.VariantIndexTask',
            spark_config='etl-index-task',
            arguments=[es_port(), release_id(), 'gene_centric', f'config/{env}-{project()}.conf',
                       f's3a://cqdg-{env}-app-datalake/es_index/gene_centric/', "all", config.es_url],
        )

        gene_suggestions_task = SparkOperator(
            task_id=f'gene_suggestions_index',
            name=f'etl-index-gene-suggestions-index',
            k8s_context=K8sContext.DEFAULT,
            spark_jar=config.spark_index_jar,
            spark_class='bio.ferlab.fhir.etl.VariantIndexTask',
            spark_config='etl-index-task',
            arguments=[es_port(), release_id(), 'gene_suggestions', f'config/{env}-{project()}.conf',
                       f's3a://cqdg-{env}-app-datalake/es_index/gene_suggestions/', "all", config.es_url],
        )
        return gene_centric_task >> gene_suggestions_task


    def variants(c):
        with TaskGroup(group_id=f'variant_index-{c}') as variant_index:
            variant_centric_task = SparkOperator(
                task_id=f'variant_centric_index',
                name=f'etl-index-variant-centric-index',
                k8s_context=K8sContext.DEFAULT,
                spark_jar=config.spark_index_jar,
                spark_class='bio.ferlab.fhir.etl.VariantIndexTask',
                spark_config='etl-index-task',
                arguments=[es_port(), release_id(), 'variant_centric', f'config/{env}-{project()}.conf',
                           f's3a://cqdg-{env}-app-datalake/es_index/variant_centric/', c, config.es_url],
            )

            variants_suggestions_task = SparkOperator(
                task_id=f'variant_suggestions_index',
                name=f'etl-index-variant-suggestions-index',
                k8s_context=K8sContext.DEFAULT,
                spark_jar=config.spark_index_jar,
                spark_class='bio.ferlab.fhir.etl.VariantIndexTask',
                spark_config='etl-index-task',
                arguments=[es_port(), release_id(), 'variant_suggestions', f'config/{env}-{project()}.conf',
                           f's3a://cqdg-{env}-app-datalake/es_index/variant_suggestions/', c, config.es_url],
            )
            variant_centric_task >> variants_suggestions_task
        return variant_index


    task_arr = [genes()]

    for chr in chromosomes:
        task = variants(chr)
        task_arr[-1] >> task
        task_arr.append(task)
