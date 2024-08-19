from datetime import datetime

from airflow import DAG
from airflow.models.param import Param
from airflow.utils.task_group import TaskGroup

from es_templates_update import es_templates_update
from etl_enrich_specimens import  etl_enrich_specimens
from etl_enrich_variants import variant_task_enrich_variants, variant_task_enrich_consequences
from etl_index import index_operator
from etl_index_variants import index_variants
from etl_normalize_variants import normalize_variant_operator
from etl_prepare_index_variants import etl_variant_prepared
from etl_publish import publish_task

with DAG(
        dag_id='etl-variant',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        params={
            'study_code': Param('CAG', type='string'), #FIXME study Codes vs study code !!!
            'owner': Param('jmichaud', type='string'),
            'dataset': Param('dataset_default', type='string'),
            'batch': Param('annotated_vcf', type='string'),
            'release_id': Param('7', type='string'),
            'study_codes': Param('CAG', type='string'),
            'project': Param('cqdg', type='string'),
            'es_port': Param('9200', type='string')
        },
) as dag:

    with TaskGroup(group_id='normalize') as normalize:
        normalize_variant_operator('snv') >> normalize_variant_operator('consequences')

    with TaskGroup(group_id='enrich') as enrich:
        variant_task_enrich_variants() >> variant_task_enrich_consequences()

    with TaskGroup(group_id='prepared') as prepared:
        etl_variant_prepared('variant_centric') >> etl_variant_prepared('gene_centric') >> etl_variant_prepared('variant_suggestions') >> etl_variant_prepared('gene_suggestions')

    with TaskGroup(group_id='index') as index:
        index_variants()

    etl_enrich_specimens() >> normalize >> enrich >> prepared >> es_templates_update() >> index >> publish_task('variant_centric,variant_suggestions,gene_centric,gene_suggestions')
