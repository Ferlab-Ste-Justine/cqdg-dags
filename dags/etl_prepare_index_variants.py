from airflow import DAG
from airflow.models.param import Param
from datetime import datetime
from lib.config import etl_variant_config, default_config_file
from lib.operators.spark import SparkOperator

etl_variant_prepared_config = etl_variant_config \
    .with_spark_class('bio.ferlab.etl.prepared.RunPrepared') \
    .args('--config', default_config_file,
          '--steps', 'default'
    )
with DAG(
        dag_id='etl-prepare-index-variant',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        params={
            'project': Param('cqdg', type='string'),
        },
) as dag:

    variant_task_variant_centric = etl_variant_prepared_config.prepend_args('variant_centric').operator(
        task_id='variant_task_variant_centric',
        name='etl-variant_task_variant_centric'
    )

    variant_task_gene_centric = etl_variant_prepared_config.prepend_args('gene_centric').operator(
        task_id='variant_task_gene_centric',
        name='etl-variant_task_gene_centric'
    )

    variant_task_variant_suggestions = etl_variant_prepared_config.prepend_args('variant_suggestions').operator(
        task_id='variant_task_variant_suggestions',
        name='etl-variant_variant_suggestions'
    )

    variant_task_gene_suggestions = etl_variant_prepared_config.prepend_args('gene_suggestions').operator(
        task_id='variant_task_gene_suggestions',
        name='etl-variant_gene_suggestions'
    )

    variant_task_variant_centric >> variant_task_gene_centric >> variant_task_variant_suggestions >> variant_task_gene_suggestions
