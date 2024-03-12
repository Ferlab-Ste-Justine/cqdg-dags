from airflow import DAG
from airflow.models import Variable
from datetime import datetime
from lib.config import keycloak_client_secret_name, env, Env, kube_config, es_url, es_port, es_credentials_secret_name,es_credentials_secret_key_password, es_credentials_secret_key_username
from lib.operators.arranger import ArrangerConfig

arranger_config = ArrangerConfig(
    es_url = es_url,
    node_environment= 'production' if env == Env.PROD else env,
    kube_config = kube_config,
    image = Variable.get('arranger_image'),
    es_port = es_port,
    es_cert_secret_name = 'opensearch-ca-certificate',
    es_credentials_secret_name = es_credentials_secret_name,
    es_credentials_secret_key_username = es_credentials_secret_key_username,
    es_credentials_secret_key_password = es_credentials_secret_key_password,
    keycloak_client_secret_name = keycloak_client_secret_name,
)

def arranger_task():
    return arranger_config.args(
              '--experimental-modules=node',
              '--es-module-specifier-resolution=node',
              'admin/run.mjs') \
              .operator(
                task_id='arranger_update_project',
                name='etl-publish-arranger-update-project',
              )
with DAG(
        dag_id='update-arranger-project',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        params={},
) as dag:
    arranger_task()

