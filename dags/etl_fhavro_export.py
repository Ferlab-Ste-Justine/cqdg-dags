from airflow import DAG
from airflow.models import Param, Variable
from datetime import datetime
from lib.config import env, fhir_url, datalake_bucket, keycloak_client_secret_name, keycloak_url, \
    aws_secret_name, aws_secret_access_key, aws_secret_secret_key, kube_config, aws_endpoint, study_codes
from lib.operators.fhavro import FhavroConfig

fhavro_config = FhavroConfig(
    fhir_url=fhir_url,
    bucket_name=datalake_bucket,
    keycloak_client_secret_name = keycloak_client_secret_name,
    keycloak_url=keycloak_url,
    aws_endpoint= aws_endpoint,
    aws_credentials_secret_name= aws_secret_name,
    aws_credentials_secret_access_key=aws_secret_access_key,
    aws_credentials_secret_secret_key=aws_secret_secret_key,
    kube_config=kube_config,
    image=Variable.get('fhavro_export_image') 
)

def fhavro_export():
    return fhavro_config.args(study_codes, env).operator(
        task_id='fhavro_export',
        name='etl-fhavro_export'
    )

with DAG(
        dag_id='fhavro-export',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        params={
            'study_codes': Param('CAG', type='string'),
            'project': Param('cqdg', type='string'),
        },
) as dag:
    fhavro_export()
