from airflow import DAG
from airflow.models.param import Param
from datetime import datetime
from lib.config import release_id, batch, kube_config, default_config_file, study_id, datalake_bucket, aws_secret_name, aws_secret_access_key, aws_secret_secret_key, aws_endpoint, hive_metastore_uri, spark_large_conf
from kubernetes.client import models as k8s
from lib.operators.spark import SparkOperatorConfig
spark_default_conf = {
 'spark.sql.shuffle.partitions' : '1000',
 'spark.sql.extensions' : 'io.delta.sql.DeltaSparkSessionExtension',
 'spark.sql.catalog.spark_catalog' : 'org.apache.spark.sql.delta.catalog.DeltaCatalog',
 'spark.hadoop.fs.s3a.impl' : 'org.apache.hadoop.fs.s3a.S3AFileSystem',
 'spark.hadoop.fs.s3a.fast.upload' : 'true',
 'spark.hadoop.fs.s3a.connection.ssl.enabled' : 'true',
 'spark.hadoop.fs.s3a.path.style.access' : 'true',
 'spark.hadoop.fs.s3a.endpoint' : aws_endpoint,
 'spark.hadoop.fs.s3a.aws.credentials.provider' : 'com.amazonaws.auth.EnvironmentVariableCredentialsProvider',
 'spark.kubernetes.driver.secretKeyRef.AWS_ACCESS_KEY_ID' : f'{aws_secret_name}:{aws_secret_access_key}',
 'spark.kubernetes.driver.secretKeyRef.AWS_SECRET_ACCESS_KEY' : f'{aws_secret_name}:{aws_secret_secret_key}',
 'spark.kubernetes.executor.secretKeyRef.AWS_ACCESS_KEY_ID' : f'{aws_secret_name}:{aws_secret_access_key}',
 'spark.kubernetes.executor.secretKeyRef.AWS_SECRET_ACCESS_KEY' : f'{aws_secret_name}:{aws_secret_secret_key}', 
 'spark.hadoop.hive.metastore.uris' : hive_metastore_uri,
 'spark.sql.warehouse.dir' : f's3a://{datalake_bucket}/hive',
 'spark.eventLog.enabled' : 'true',
 'spark.eventLog.dir' : f's3a://{datalake_bucket}/spark-logs',
 'spark.driver.extraJavaOptions' : '"-Divy.cache.dir=/tmp -Divy.home=/tmp"',
 'spark.jars.ivy': '/tmp'
}

normalized_etl= SparkOperatorConfig(
                    spark_configs=[spark_default_conf, spark_large_conf],
                    image = 'apache/spark:3.4.1',
                    kube_config=kube_config,
                    is_delete_operator_pod=False
            ) \
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
.with_spark_jar('s3a://cqdg-qa-app-datalake/jars/variant-task.jar') \
.add_package('io.delta:delta-core_2.12:2.3.0') \
.add_spark_conf({
    # 'spark.jars.packages': 'io.delta:delta-core_2.12:2.3.0',
    # 'spark.jars.packages': 'io.delta:delta-core_2.12:2.1.1,io.projectglow:glow-spark3_2.12:1.2.1',
    # 'spark.jars.excludes':'org.apache.hadoop:hadoop-client',
    'spark.kubernetes.container.image': 'ferlabcrsj/spark:469f2fc61f06fcfa73c1480a5e5f73f59768152d',
    # 'spark.kubernetes.container.image': 'apache/spark:3.4.1', 
    # 'spark.kubernetes.container.image': 'apache/spark:v3.3.2',
    # 'spark.kubernetes.file.upload.path': f's3a://{datalake_bucket}/dependencies'    
}) #.with_spark_jar('https://github.com/Ferlab-Ste-Justine/etl-cqdg-portal/releases/download/v2.21.5/variant-task.jar') 

### 
# - ferlabcrsj/spark:469f2fc61f06fcfa73c1480a5e5f73f59768152d (spark 3.3.2 + hadoop-aws) + https://github.com/Ferlab-Ste-Justine/etl-cqdg-portal/releases/download/v2.21.5/variant-task.jar => fonctionne
# - apache/spark:3.3.2 + https://github.com/Ferlab-Ste-Justine/etl-cqdg-portal/releases/download/v2.21.5/variant-task.jar  + hadoop-aws dans packages => ne marche pas car probleme classpath ( java.lang.ClassNotFoundException: Class org.apache.hadoop.fs.s3a.S3AFileSystem not found)
# - apache/spark:3.4.1 + https://github.com/Ferlab-Ste-Justine/etl-cqdg-portal/releases/download/v2.21.5/variant-task.jar => ne fonctionne pas car incompatibilité version delta 
# - ferlabcrsj/spark:469f2fc61f06fcfa73c1480a5e5f73f59768152d (spark 3.3.2 + hadoop-aws) + new variant-task.jar delta 2.1.1 => ne fonctionne pas :  java.lang.NoClassDefFoundError: io/delta/tables/DeltaTable$


#  s3a://cqdg-qa-app-datalake/jars/variant-task.jar
###

def normalize_variant_operator(name):
    etl = normalized_etl.args('--release-id', release_id ) if name == 'snv' else normalized_etl
    return etl.prepend_args(name).operator(
        task_id=f'normalize-{name}',
        name=f'normalize-{name}',
        env_vars=[
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
    normalize_variant_operator('snv') >>  normalize_variant_operator('consequences') 
