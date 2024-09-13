from datetime import datetime

from airflow import DAG
from airflow.models.param import Param

from lib.config import etl_variant_config, default_config_file
from lib.slack import Slack

etl_variant_enrich_config = etl_variant_config \
    .with_spark_class('bio.ferlab.etl.enriched.RunEnrichGenomic') \
    .args('--config', default_config_file,
          '--steps', 'default'
    )

def variant_task_enrich_variants():
    return etl_variant_enrich_config.prepend_args('variants').operator(
        task_id='variant_task_variant_enrich_variants',
        name='etl-variant_task_variant_enrich_variants'
    )

def variant_task_enrich_consequences():
    return etl_variant_enrich_config.prepend_args('consequences').operator(
        task_id='variant_task_variant_enrich_consequences',
        name='etl-variant_task_variant_enrich_consequences'
    )

with DAG(
        dag_id='etl-enrich-variants',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        params={
            'project': Param('cqdg', type='string'),
        },
        on_failure_callback=Slack.notify_task_failure
) as dag:
    variant_task_enrich_variants() >> variant_task_enrich_consequences()
