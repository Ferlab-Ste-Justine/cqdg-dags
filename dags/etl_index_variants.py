from airflow import DAG
from airflow.models.param import Param
from airflow.utils.task_group import TaskGroup
from datetime import datetime
from lib.config import es_port, es_url, release_id, default_config_file, etl_index_config, datalake_bucket, env, Env
from lib.slack import Slack

etl_index_variant_config = etl_index_config \
    .with_spark_class('bio.ferlab.fhir.etl.VariantIndexTask')

def operator(indexName:str, chromosome:str='all'):
    indexNameU = indexName.replace('_', '-')
    return etl_index_variant_config \
        .args(es_port, release_id, indexName, default_config_file, f's3a://{datalake_bucket}/es_index/{indexName}/', chromosome, es_url) \
        .operator(
        task_id=f'{indexName}_index',
        name=f'etl-index-{indexNameU}-index'
    )

def genes():
    return operator('gene_centric') >> operator('gene_suggestions')


def variants(chr):
    with TaskGroup(group_id=f'variant_index-{chr}') as variant_index:
        operator('variant_centric', chr) >> operator('variant_suggestions', chr)
    return variant_index


def index_variants():
    if env == Env.QA:
        chromosomes = ['1']
    else:
        chromosomes = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18',
                       '19', '20', '21', '22', 'X', 'Y', 'M']

    task_arr = [genes()]

    for chromosome in chromosomes:
        task = variants(chromosome)
        task_arr[-1] >> task
        task_arr.append(task)
    return task_arr


with DAG(
        dag_id='etl-index-variants',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        params={
            'release_id': Param('7', type='string'),
            'project': Param('cqdg', type='string')
        },
        on_failure_callback=Slack.notify_task_failure
) as dag:
    index_variants()