from datetime import datetime

from airflow import DAG
from airflow.models import Param, Variable

from cqdg.lib.config import fhir_url, keycloak_url, aws_secret_name, aws_secret_secret_key, clinical_data_bucket, \
    kube_config, keycloak_client_resource_secret_name, \
    aws_endpoint, study_code
from cqdg.lib.operators.drs_import import DrsImportConfig

drs_import_config = DrsImportConfig(
    fhir_url=fhir_url,
    ferload_url=Variable.get('ferload_url'),
    keycloak_client_secret_name=keycloak_client_resource_secret_name,
    keycloak_url=keycloak_url,
    aws_endpoint=aws_endpoint,
    aws_credentials_secret_name=aws_secret_name,
    aws_credentials_secret_secret_key=aws_secret_secret_key,
    clinical_data_bucket_name=clinical_data_bucket,
    kube_config=kube_config,
    image=Variable.get('ferload_drs_import_image')
).args("bio/ferlab/cqdg/ferload/FerloadImport")

def ferload_drs_import():
    return drs_import_config.args(study_code).operator(
        task_id='drs_import',
        name='ferload-drs_import'
    )

with DAG(
        dag_id='drs_import',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        params={
            'study_code': Param('CAG', type='string'),
        },
) as dag:
    ferload_drs_import()
