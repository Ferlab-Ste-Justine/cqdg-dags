from datetime import datetime

from airflow import DAG
from airflow.models.param import Param

from etl_prepare_index import etl_base_config, spark_small_conf, prepare_index_jar
from lib.config import default_config_file, study_code


def etl_enrich_specimens():
    return etl_base_config \
        .add_spark_conf(spark_small_conf) \
        .with_spark_jar(prepare_index_jar) \
        .with_spark_class('bio.ferlab.fhir.etl.Enrich') \
        .args(
        '--config', default_config_file,
        '--steps', 'default',
        '--app-name', 'enrich_specimen',
        '--study-id', study_code,
    ).operator(
        task_id='enrich-specimen',
        name='etl-enrich-specimen'
    )

with DAG(
        dag_id='etl-enrich-specimen',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        params={
            'study_codes': Param('CAG', type='string'),
            'project': Param('cqdg', type='string'),
        },
) as dag:
    etl_enrich_specimens()

