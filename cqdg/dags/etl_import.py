from airflow import DAG
from airflow.models.param import Param
from datetime import datetime
from cqdg.lib.config import etl_base_config, spark_small_conf, import_jar, default_config_file, study_codes
from cqdg.lib.slack import Slack


def etl_import():
    return etl_base_config \
        .with_spark_class('bio.ferlab.fhir.etl.ImportTask') \
        .with_spark_jar(import_jar) \
        .add_spark_conf(spark_small_conf) \
        .args(default_config_file, 'default', study_codes) \
        .operator(
            task_id='import_task',
            name='etl-import-task',
            on_failure_callback=Slack.notify_task_failure
    )

with DAG(
        dag_id='etl-import',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        params={
            'study_codes': Param('CAG', type='string'),
            'project': Param('cqdg', type='string'),
        },
) as dag:
    etl_import()
