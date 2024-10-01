from datetime import datetime

from airflow import DAG
from airflow.models.param import Param

from lib.config import etl_variant_config, default_config_file
from lib.slack import Slack

etl_variant_prepared_config = etl_variant_config \
    .with_spark_class('bio.ferlab.etl.prepared.RunPrepared') \
    .args('--config', default_config_file,
          '--steps', 'default'
    )

def etl_variant_prepared(name):
    return etl_variant_prepared_config.prepend_args(name).operator(
        task_id=f'variant_task_{name}',
        name=f'etl-variant_task_{name}'
    )

with DAG(
        dag_id='etl-prepare-index-variant',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        params={
            'project': Param('cqdg', type='string'),
        },
        on_failure_callback=Slack.notify_task_failure
) as dag:

    etl_variant_prepared('variant_centric') >> etl_variant_prepared('gene_centric') >> etl_variant_prepared('variant_suggestions') >> etl_variant_prepared('gene_suggestions')
