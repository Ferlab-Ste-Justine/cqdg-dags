from airflow import DAG
from airflow.models import Param, Variable
from datetime import datetime
from lib.config import kube_config, es_url, es_port, es_credentials_secret_name, \
    es_credentials_secret_key_password, es_credentials_secret_key_username, release_id, study_codes
from lib.operators.publish import PublishConfig

etl_publish_config = PublishConfig(
    es_url = es_url,
    kube_config = kube_config,
    image = Variable.get('publish_image'),
    es_port = es_port,
    es_cert_secret_name = 'opensearch-ca-certificate',
    es_credentials_secret_name = es_credentials_secret_name,
    es_credentials_secret_key_username = es_credentials_secret_key_username,
    es_credentials_secret_key_password = es_credentials_secret_key_password,
)

def publish_task(job_types: str):
    return etl_publish_config.args(
              '-n', es_url,
              '-p', es_port,
              '-r', release_id,
              '-j', job_types) \
              .operator(
                task_id='etl_publish_variant',
                name='etl-publish_variant',
              )
with DAG(
        dag_id='etl-publish-variant',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        params={
            'es_port': Param('9200', type='string'),
            'release_id': Param('0', type='string'),
            'job_types': Param('variant_centric,variant_suggestions,gene_centric,gene_suggestions', type='string'),
        },
) as dag:
    publish_task('{{ params.job_types }}')

