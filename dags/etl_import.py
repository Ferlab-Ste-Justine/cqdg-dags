from airflow import DAG
from airflow.models.param import Param
from datetime import datetime
from lib.config import etl_deps_config, spark_small_conf, import_jar, default_config_file, release_id, study_ids
from lib.operators.spark import SparkOperator

def etl_import():
    return etl_deps_config \
        .with_spark_class('bio.ferlab.fhir.etl.ImportTask') \
        .with_spark_jar(import_jar) \
         .add_spark_conf(spark_small_conf) \
         .args(default_config_file, 'default', release_id, study_ids) \
         .operator(
              task_id='import_task',
              name='etl-import-task'
          )    

with DAG(
        dag_id='etl-import',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        params={
            'release_id': Param('7', type='string'),
            'study_ids': Param('ST0000017', type='string'),
            'project': Param('cqdg', type='string'),
        },
) as dag:
    etl_import()
