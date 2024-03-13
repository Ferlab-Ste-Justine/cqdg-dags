from datetime import datetime

from airflow import DAG
from airflow.models import Param
from kubernetes.client import models as k8s

from lib.config import release_id, batch, default_config_file, study_id, datalake_bucket, aws_secret_name, \
    aws_secret_access_key, aws_secret_secret_key, etl_base_config

"""
!!!WARNING Before changing this configuration, read this :
Normalize variant has a dependency on Glow 1.2.1 which is NOT compatible with Spark 3.4. 
- We use our ETL image as 
a client to get the jar, and pass file://path_to_jar in spark submit argument 
- Spark client copy this jar in S3 (spark.kubernetes.file.upload.path configuration) to share it with driver. 
So AWS S3 credentials are required by the client.
- For driver and executors we use our own Ferlab image
based on apache/spark:v3.3.2. This image just add hadoop-aws dependencies because glow requires to have these jars in 
spark jars directory. Use packages configuration in spark job is NOT enough.
- variant-normalize-task.jar embed delta-core, otherwise we got ClassNotFoundException on DeltaTable, 
even if delta is specified in packages configuration. We think this is an issue due to spark:v3.2.2 image, but not sure.
Glow 2.0.0 should fixed these issues, but it is not yet available on Maven central.
"""
normalized_etl = etl_base_config \
    .with_spark_class('bio.ferlab.etl.normalized.RunNormalizedGenomic').args(
    '--config', default_config_file,
    '--steps', 'default',
    '--app-name', 'variant_task_consequences',
    '--owner', '{{ params.owner }}',
    '--dataset', '{{ params.dataset }}',
    '--batch', batch,
    '--study-id', study_id,
    '--study-code', '{{ params.study_code }}'
) \
    .with_spark_jar(
    # ETL client container contain this file locally, it will be share with driver through spark.kubernetes.file.upload.path
    'file:///app/variant-normalize-task.jar'
) \
    .add_spark_conf({
    'spark.jars.packages': 'org.apache.hadoop:hadoop-aws:3.3.2',
    # required by the client to be able to push etl jar file on s3
    'spark.kubernetes.container.image': 'ferlabcrsj/spark:469f2fc61f06fcfa73c1480a5e5f73f59768152d',
    # use ferlab spark 3.3.2 image because it contains hadoop-aws in spark jars directory, otherwise glow give a ClassNotFoundException
    'spark.kubernetes.file.upload.path': f's3a://{datalake_bucket}/dependencies'  # directory used to share etl jar
})


def normalize_variant_operator(name):
    etl = normalized_etl.args('--release-id', release_id) if name == 'snv' else normalized_etl
    return etl.prepend_args(name).operator(
        task_id=f'normalize-{name}',
        name=f'normalize-{name}',
        env_vars=[
            # Client needs spark credentials in order to push jar file in spark.kubernetes.file.upload.path
            k8s.V1EnvVar(
                name='AWS_ACCESS_KEY_ID',
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name=aws_secret_name,
                        key=aws_secret_access_key,
                    ),
                ),
            ),
            k8s.V1EnvVar(
                name='AWS_SECRET_ACCESS_KEY',
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name=aws_secret_name,
                        key=aws_secret_secret_key,
                    ),
                ),
            ), ]
    )


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
