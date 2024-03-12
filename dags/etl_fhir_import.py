from datetime import datetime

from airflow import DAG
from airflow.models import Param, Variable

from lib.config import fhir_url, keycloak_client_secret_name, keycloak_url, aws_secret_name, aws_secret_access_key, aws_secret_secret_key, clinical_data_bucket, file_import_bucket, kube_config
from lib.operators.fhir_import import FhirCsvOperator, FhirCsvConfig

fhir_import_config = FhirCsvConfig(
    fhir_url=fhir_url,
    keycloak_client_secret_name = keycloak_client_secret_name,
    keycloak_url=keycloak_url,
    aws_credentials_secret_name= aws_secret_name,
    aws_credentials_secret_access_key=aws_secret_access_key,
    aws_credentials_secret_secret_key=aws_secret_secret_key,
    clinical_data_bucket_name=clinical_data_bucket,
    file_import_bucket_name=file_import_bucket,
    id_service_url=Variable.get('id_service_url'),
    kube_config=kube_config,
    image=Variable.get('fhir_import_image') 
).args("bio/ferlab/cqdg/etl/FhirImport")

with DAG(
    dag_id='etl-fhir-import',
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    params={
        'prefix': Param('prefix', type='string'),
        'studyId': Param('7', type='string'),
        'studyVersion': Param('13', type='string'),
        'study': Param('cag', type='string'),
        'project': Param('jmichaud', type='string'),
        'is_restricted': Param('', enum=['', 'true', 'false']),
    },
) as dag:

    def prefix() -> str:
        return '{{ params.prefix }}'

    def study_clin_data_id() -> str:
        return '{{ params.studyId }}'

    def study_clin_data_version() -> str:
        return '{{ params.studyVersion }}'

    def study() -> str:
        return '{{ params.study }}'

    def project() -> str:
        return '{{ params.project }}'

    def is_restricted() -> str:
        return '{{ params.is_restricted }}'

    csv_import = fhir_import_config \
                    .args(prefix(), study_clin_data_id(), study_clin_data_version(), study(), project(), "true", is_restricted()) \
                    .operator(
                        task_id='fhir_import',
                        name='etl-fhir_import',
                    )
