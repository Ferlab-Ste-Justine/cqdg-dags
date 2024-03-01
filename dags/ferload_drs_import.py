from airflow import DAG
from airflow.models.param import Param, Variable
from datetime import datetime

from dags.lib.config import fhir_url, keycloak_client_secret_name, keycloak_url, aws_secret_name, aws_secret_access_key, \
    aws_secret_secret_key, clinical_data_bucket, kube_config, study_id, ferload_url
from dags.lib.operators.drs_import import DrsImportConfig

drs_import_config = DrsImportConfig(
    fhir_url=fhir_url,
    ferload_url=ferload_url,
    keycloak_client_secret_name=keycloak_client_secret_name,
    keycloak_url=keycloak_url,
    aws_credentials_secret_name=aws_secret_name,
    aws_credentials_secret_access_key=aws_secret_access_key,
    aws_credentials_secret_secret_key=aws_secret_secret_key,
    clinical_data_bucket_name=clinical_data_bucket,
    kube_config=kube_config,
    image=Variable.get('ferload_drs_import_image')
).args("bio/ferlab/cqdg/ferload/FerloadImport")


with DAG(
        dag_id='drs_import',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        params={
            'study_id': Param('ST0000017', type='string'),
        },
) as dag:
    csv_import = drs_import_config \
        .args(study_id) \
        .operator(
        task_id='drs_import',
        name='ferload-drs_import',
    )
