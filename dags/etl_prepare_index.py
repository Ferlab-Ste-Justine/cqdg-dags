from airflow import DAG
from airflow.models.param import Param
from datetime import datetime
from lib.config import default_config_file, etl_deps_config, spark_small_conf, prepare_index_jar, study_ids

etl_prepare_config = etl_deps_config \
    .add_spark_conf(spark_small_conf) \
    .with_spark_jar(prepare_index_jar) \
    .with_spark_class('bio.ferlab.fhir.etl.PrepareIndex') \
    .args(default_config_file, 'default', 'all', study_ids)

def prepare_index():
    return etl_prepare_config \
        .operator(
            task_id='prepare_index',
            name='etl-prepare-index'
        )
with DAG(
        dag_id='etl-prepare-index',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        params={
            'release_id': Param('7', type='string'),
            'study_ids': Param('ST0000017', type='string'),
            'project': Param('cqdg', type='string'),
        },
) as dag:
    prepare_index()

