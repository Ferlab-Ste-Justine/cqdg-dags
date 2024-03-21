from datetime import datetime

from airflow import DAG
from airflow.models.param import Param

from lib.config import default_config_file, etl_base_config, spark_medium_conf, variant_jar, study_id, dataset, batch

with DAG(
        dag_id='etl-enrich-snv',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        params={
            'study_id': Param('CAG', type='string'),
            'dataset': Param('dataset_default', type='string'),
            'batch': Param('annotated_vcf', type='string'),
            'project': Param('cqdg', type='string'),
        },
) as dag:
    variant_task_enrich_snv = etl_base_config \
        .with_spark_jar(variant_jar) \
        .with_spark_class('bio.ferlab.etl.enriched.RunEnrichGenomic') \
        .add_spark_conf(spark_medium_conf) \
        .args('snv') \
        .args(
        '--config', default_config_file,
        '--steps', 'default',
        '--app-name', 'variant_task_enrich_snv',
        '--study-id', study_id,
        '--dataset', dataset,
        '--batch', batch
    ).operator(
        task_id='variant_task_variant_enrich_snv',
        name='etl-variant_task_variant_enrich_snv'
    )

    variant_task_enrich_snv
