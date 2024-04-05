from airflow.models import Variable, Param

from lib.operators.base_kubernetes import KubeConfig
from lib.operators.spark import SparkOperatorConfig


class Env:
    QA = 'qa'
    DEV = 'dev'
    PROD = 'prod'


env = Variable.get('environment')

es_url = Variable.get('es_url')
es_port = Variable.get('es_port', '9200')
keycloak_url = Variable.get('keycloak_url')
fhir_url = Variable.get('fhir_url')

aws_secret_name = 'ceph-s3-credentials'
aws_secret_access_key = 'access'
aws_secret_secret_key = 'secret'

aws_endpoint = Variable.get('object_store_url')
hive_metastore_uri = Variable.get('hive_metastore_uri')

datalake_bucket = Variable.get('datalake_bucket')
clinical_data_bucket = Variable.get('clinical_data_bucket')
file_import_bucket = Variable.get('file_import_bucket')

keycloak_client_secret_name = 'keycloak-client-system-credentials'
keycloak_client_resource_secret_name = 'keycloak-client-resource-server-credentials'

es_credentials_secret_name = 'opensearch-dags-credentials'
es_credentials_secret_key_username = 'username'
es_credentials_secret_key_password = 'password'

default_params = {
    'study_id': Param('CAG', type='string'),
    'project': Param('cqdg', type='string'),
}

study_id = '{{ params.study_id }}'
study_ids = '{{ params.study_ids }}'
study_code = '{{ params.study_code }}'
study_codes = '{{ params.study_codes }}'
project = '{{ params.project }}'
dataset = '{{ params.dataset }}'
batch = '{{ params.batch }}'
release_id = '{{ params.release_id }}'
default_config_file = f'config/{env}-{project}.conf'

kube_config = KubeConfig(
    in_cluster=Variable.get('k8s_in_cluster', 'true').lower() == 'true',
    namespace=Variable.get('k8s_namespace', None),
    service_account_name=Variable.get('k8s_service_account_name'),
    image_pull_secrets_name=Variable.get('k8s_image_pull_secret_name', None),
    cluster_context=Variable.get('k8s_cluster_context', None)
)

javaOptsIvy = '-Divy.cache.dir=/tmp -Divy.home=/tmp'
spark_default_conf = {
    'spark.sql.shuffle.partitions': '1000',
    'spark.sql.extensions': 'io.delta.sql.DeltaSparkSessionExtension',
    'spark.sql.catalog.spark_catalog': 'org.apache.spark.sql.delta.catalog.DeltaCatalog',
    'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
    'spark.hadoop.fs.s3a.fast.upload': 'true',
    'spark.hadoop.fs.s3a.connection.ssl.enabled': 'true',
    'spark.hadoop.fs.s3a.path.style.access': 'true',
    'spark.hadoop.fs.s3a.endpoint': aws_endpoint,
    'spark.hadoop.fs.s3a.aws.credentials.provider': 'com.amazonaws.auth.EnvironmentVariableCredentialsProvider',
    'spark.kubernetes.driver.secretKeyRef.AWS_ACCESS_KEY_ID': f'{aws_secret_name}:{aws_secret_access_key}',
    'spark.kubernetes.driver.secretKeyRef.AWS_SECRET_ACCESS_KEY': f'{aws_secret_name}:{aws_secret_secret_key}',
    'spark.kubernetes.executor.secretKeyRef.AWS_ACCESS_KEY_ID': f'{aws_secret_name}:{aws_secret_access_key}',
    'spark.kubernetes.executor.secretKeyRef.AWS_SECRET_ACCESS_KEY': f'{aws_secret_name}:{aws_secret_secret_key}',
    'spark.hadoop.hive.metastore.uris': hive_metastore_uri,
    'spark.sql.warehouse.dir': f's3a://{datalake_bucket}/hive',
    'spark.eventLog.enabled': 'true',
    'spark.eventLog.dir': f's3a://{datalake_bucket}/spark-logs',
    'spark.driver.extraJavaOptions': javaOptsIvy,
    'spark.jars.ivy': '/tmp',
    'spark.log.level': 'WARN'
}

spark_small_conf = {
    'spark.driver.memory': '16g',
    'spark.driver.cores': '6',
    'spark.executor.instances': '1',
    'spark.executor.memory': '16g',
    'spark.memory.fraction': '0.9',
    'spark.memory.storageFraction': '0.1',
    'spark.executor.cores': '12',
    'spark.kubernetes.executor.volumes.emptyDir.spark-local-dir-1.options.sizeLimit': '50Gi',
    'spark.kubernetes.executor.volumes.emptyDir.spark-local-dir-1.mount.path': '/data',
    'spark.kubernetes.executor.volumes.emptyDir.spark-local-dir-1.mount.readOnly': 'false',
}

spark_medium_conf = {
    'spark.driver.memory': '16g',
    'spark.driver.cores': '6',
    'spark.executor.instances': '2',
    'spark.executor.memory': '16g',
    'spark.memory.fraction': '0.9',
    'spark.memory.storageFraction': '0.1',
    'spark.executor.cores': '12',
    'spark.kubernetes.executor.volumes.emptyDir.spark-local-dir-1.options.sizeLimit': '350Gi',
    'spark.kubernetes.executor.volumes.emptyDir.spark-local-dir-1.mount.path': '/data',
    'spark.kubernetes.executor.volumes.emptyDir.spark-local-dir-1.mount.readOnly': 'false',
}

spark_large_conf = {
    'spark.driver.memory': '70g',
    'spark.driver.cores': '12',
    'spark.executor.instances': '20',
    'spark.executor.memory': '64g',
    'spark.memory.fraction': '0.9',
    'spark.memory.storageFraction': '0.1',
    'spark.executor.cores': '12',
    'spark.kubernetes.executor.volumes.emptyDir.spark-local-dir-1.options.sizeLimit': '350Gi',
    'spark.kubernetes.executor.volumes.emptyDir.spark-local-dir-1.mount.path': '/data',
    'spark.kubernetes.executor.volumes.emptyDir.spark-local-dir-1.mount.readOnly': 'false',
}

javaOptsEsCert = (f'{javaOptsIvy} -Djavax.net.ssl.trustStore=/opt/keystores/truststore.p12 '
                  f'-Djavax.net.ssl.trustStorePassword=changeit')

spark_index_conf = {
    'spark.kubernetes.driver.podTemplateFile': 'local:///app/pod-template-es-cert.yml',
    'spark.driver.extraJavaOptions': javaOptsEsCert,
    'spark.kubernetes.driver.secretKeyRef.ES_USERNAME': f'{es_credentials_secret_name}:{es_credentials_secret_key_username}',
    'spark.kubernetes.driver.secretKeyRef.ES_PASSWORD': f'{es_credentials_secret_name}:{es_credentials_secret_key_password}',
    'spark.kubernetes.executor.podTemplateFile': 'local:///app/pod-template-es-cert.yml',
    'spark.executor.extraJavaOptions': javaOptsEsCert,
    'spark.kubernetes.executor.secretKeyRef.ES_USERNAME': f'{es_credentials_secret_name}:{es_credentials_secret_key_username}',
    'spark.kubernetes.executor.secretKeyRef.ES_PASSWORD': f'{es_credentials_secret_name}:{es_credentials_secret_key_password}'
}

variant_jar = 'local:///app/variant-task.jar'
prepare_index_jar = 'local:///app/prepare-index.jar'
import_jar = 'local:///app/import-task.jar'
index_jar = 'local:///app/index-task.jar'
publish_jar = 'local:///app/publish-task.jar'

etl_base_config = SparkOperatorConfig(
    spark_configs=[spark_default_conf],
    image=Variable.get('etl_image'),
    kube_config=kube_config
).add_packages('org.apache.hadoop:hadoop-aws:3.3.4', 'io.delta:delta-spark_2.12:3.1.0')

etl_index_config = etl_base_config \
    .add_spark_conf(spark_small_conf, spark_index_conf) \
    .with_spark_jar(index_jar)

etl_publish_config = etl_base_config \
    .add_spark_conf(spark_small_conf, spark_index_conf) \
    .with_spark_jar(publish_jar) \
    .with_spark_class('bio.ferlab.fhir.etl.PublishTask')

etl_variant_config = etl_base_config \
    .add_spark_conf(spark_large_conf) \
    .with_spark_jar(variant_jar)
