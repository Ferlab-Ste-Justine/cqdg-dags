from datetime import datetime

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

from cqdg.dags.es_templates_update import es_templates_update
from cqdg.dags.etl_enrich_specimens import etl_enrich_specimens
from cqdg.dags.etl_enrich_variants import variant_task_enrich_variants, variant_task_enrich_consequences
from cqdg.dags.etl_index_variants import index_variants
from cqdg.dags.etl_normalize_variants import extract_params, normalized_etl
from cqdg.dags.etl_prepare_index_variants import etl_variant_prepared
from cqdg.dags.etl_publish_variants import publish_task
from cqdg.lib.slack import Slack

with DAG(
        dag_id='etl-variant',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        # concurrency set to 1, only one task can run at a time to avoid conflicts in Delta table
        concurrency=1,
        params={
            'study_code': Param('CAG', type='string'),
            'owner': Param('jmichaud', type='string'),
            'dateset_batches': Param(
                [
                    {'dataset': 'dataset_dataset1', 'batches': ['annotated_vcf1','annotated_vcf2']},
                    {'dataset': 'dataset_dataset2', 'batches': ['annotated_vcf']}
                ],
                schema =  {
                    "type": "array",
                    "minItems": 1,
                    "items": {
                        "type": "object",
                        "default": {'dataset': 'dataset_default', 'batches': ['annotated_vcf']},
                        "properties": {
                            "dataset": {"type": "string"},
                            "batches": {"type": "array", "items": {"type": "string"}},
                        },
                        "required": ["dataset", "batches"]
                    },
                }
            ),
            'release_id': Param('7', type='string'),
            'project': Param('cqdg', type='string'),
            'es_port': Param('9200', type='string'),
        },
) as dag:
    params = extract_params()

    with TaskGroup(group_id='normalize') as normalize:
        normalized_etl(run_time_params = params, name='snv') >> normalized_etl(run_time_params = params, name='consequences')

    with TaskGroup(group_id='enrich') as enrich:
        variant_task_enrich_variants() >> variant_task_enrich_consequences()

    with TaskGroup(group_id='prepared') as prepared:
        etl_variant_prepared('variant_centric') >> etl_variant_prepared('gene_centric') >> etl_variant_prepared('variant_suggestions') >> etl_variant_prepared('gene_suggestions')

    with TaskGroup(group_id='index') as index:
        index_variants()

    start = EmptyOperator(
        task_id="start",
        on_success_callback=Slack.notify_dag_start
    )

    slack = EmptyOperator(
        task_id="slack",
        on_success_callback=Slack.notify_dag_completion
    )


start >> etl_enrich_specimens() >> normalize >> enrich >> prepared >> es_templates_update() >> index >> publish_task('variant_centric,variant_suggestions,gene_centric,gene_suggestions') >> slack
