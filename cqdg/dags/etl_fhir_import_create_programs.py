from datetime import datetime

from airflow import DAG
from airflow.models import Variable

from cqdg.lib.config import fhir_url, keycloak_client_secret_name, keycloak_url, aws_secret_name, aws_secret_access_key, \
    aws_secret_secret_key, file_import_bucket, kube_config, aws_endpoint, public_bucket
from cqdg.lib.operators.fhir_import import FhirCsvConfig

fhir_import_config = FhirCsvConfig(
    fhir_url=fhir_url,
    keycloak_client_secret_name=keycloak_client_secret_name,
    keycloak_url=keycloak_url,
    aws_endpoint=aws_endpoint,
    aws_credentials_secret_name=aws_secret_name,
    aws_credentials_secret_access_key=aws_secret_access_key,
    aws_credentials_secret_secret_key=aws_secret_secret_key,
    clinical_data_bucket_name=public_bucket,
    file_import_bucket_name=file_import_bucket,
    id_service_url=Variable.get('id_service_url'),
    kube_config=kube_config,
    image=Variable.get('fhir_import_image')
).args("bio/ferlab/cqdg/etl/FhirCreatePrograms")

with DAG(
        dag_id='etl-fhir-import-create-programs',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        params={},
) as dag:
    fhir_import_config.operator(
        task_id='fhir_import_create_programs',
        name='etl-fhir_import-create-programs',
    )
