from datetime import datetime

from airflow import DAG
from airflow.models import Param

from lib.config import batch, default_config_file, study_code, spark_large_conf, \
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
        '--study-code', study_code
    ) \
    .add_package('io.projectglow:glow-spark3_2.12:2.0.0') \
    .add_spark_conf({'spark.jars.excludes': 'org.apache.hadoop:hadoop-client,'
                                            'io.netty:netty-all,'
                                            'io.netty:netty-handler,'
                                            'io.netty:netty-transport-native-epoll',
                     'spark.hadoop.io.compression.codecs': 'io.projectglow.sql.util.BGZFCodec',
                     },
                    spark_large_conf) \


def normalize_variant_operator(name):
    etl = normalized_etl if name == 'snv' else normalized_etl
    return etl.prepend_args(name).operator(
        task_id=f'normalize-{name}',
        name=f'normalize-{name}')


with DAG(
        dag_id='etl-normalize-variants',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        params={
            'study_code': Param('CAG', type='string'),
            'owner': Param('jmichaud', type='string'),
            'dataset': Param('dataset_default', type='string'),
            'batch': Param('annotated_vcf', type='string'),
            'project': Param('cqdg', type='string'),
        },
) as dag:
    normalize_variant_operator('snv') >> normalize_variant_operator('consequences')
