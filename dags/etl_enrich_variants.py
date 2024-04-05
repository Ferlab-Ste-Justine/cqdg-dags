from airflow import DAG
from airflow.models.param import Param
from datetime import datetime
from lib.config import etl_variant_config, default_config_file
from lib.operators.spark import SparkOperator

etl_variant_enrich_config = etl_variant_config \
    .with_spark_class('bio.ferlab.etl.enriched.RunEnrichGenomic') \
    .args('--config', default_config_file,
          '--steps', 'default'
    )
with DAG(
        dag_id='etl-enrich-variants',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        params={
            'project': Param('cqdg', type='string'),
        },
) as dag:

    variant_task_enrich_variants = etl_variant_enrich_config.prepend_args('variants').operator(        
        task_id='variant_task_variant_enrich_variants',
        name='etl-variant_task_variant_enrich_variants'
    )

    variant_task_enrich_consequences = etl_variant_enrich_config.prepend_args('consequences').operator(
        task_id='variant_task_variant_enrich_consequences',
        name='etl-variant_task_variant_enrich_consequences'
    )


    variant_task_enrich_variants >> variant_task_enrich_consequences
