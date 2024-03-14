from airflow import DAG
from airflow.models.param import Param
from datetime import datetime
from lib.config import default_config_file, study_ids
from etl_prepare_index import etl_base_config, spark_small_conf, prepare_index_jar

with DAG(
        dag_id='etl-enrich-specimen',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        params={
            'study_ids': Param('ST0000042', type='string'),
            'project': Param('cqdg', type='string'),
        },
) as dag:

    etl_base_config \
        .add_spark_conf(spark_small_conf) \
        .with_spark_jar(prepare_index_jar) \
        .with_spark_class('bio.ferlab.fhir.etl.Enrich') \
        .args(
            '--config', default_config_file,
            '--steps', 'default',
            '--app-name', 'enrich_specimen',
            '--study-id', study_ids
        ).operator(
            task_id='enrich-specimen',
            name='etl-enrich-specimen'
        )
    

