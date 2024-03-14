from datetime import datetime

from airflow import DAG
from airflow.models import Param

from lib.config import release_id, batch, default_config_file, study_id, spark_large_conf, \
    etl_variant_config, spark_small_conf

normalized_etl = etl_variant_config \
    .with_spark_class('bio.ferlab.etl.normalized.RunNormalizedGenomic') \
    .args(
        '--config', default_config_file,
        '--steps', 'default',
        '--app-name', 'variant_task_consequences',
        '--owner', '{{ params.owner }}',
        '--dataset', '{{ params.dataset }}',
        '--batch', batch,
        '--study-id', study_id,
        '--study-code', '{{ params.study_code }}'
    ) \
    .add_package('io.projectglow:glow-spark3_2.12:2.0.0') \
    .add_spark_conf({'spark.jars.excludes': 'org.apache.hadoop:hadoop-client,'
                                            'io.netty:netty-all,'
                                            'io.netty:netty-handler,'
                                            'io.netty:netty-transport-native-epoll'},
                    spark_small_conf) \


def normalize_variant_operator(name):
    etl = normalized_etl.args('--release-id', release_id) if name == 'snv' else normalized_etl
    return etl.prepend_args(name).operator(
        task_id=f'normalize-{name}',
        name=f'normalize-{name}')


with DAG(
        dag_id='etl-normalize-variants',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        params={
            'study_id': Param('ST0000002', type='string'),
            'study_code': Param('study1', type='string'),
            'owner': Param('jmichaud', type='string'),
            'release_id': Param('1', type='string'),
            'dataset': Param('dataset_default', type='string'),
            'batch': Param('annotated_vcf', type='string'),
            'project': Param('cqdg', type='string'),
        },
) as dag:
    normalize_variant_operator('snv') >> normalize_variant_operator('consequences')
