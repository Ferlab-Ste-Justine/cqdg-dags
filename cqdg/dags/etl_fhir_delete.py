from datetime import datetime

from airflow import DAG
from airflow.models import Param, Variable

from cqdg.lib.config import (
    aws_endpoint,
    aws_secret_access_key,
    aws_secret_name,
    aws_secret_secret_key,
    clinical_data_bucket,
    fhir_url,
    file_import_bucket,
    keycloak_client_secret_name,
    keycloak_url,
    kube_config,
    study_code,
)
from cqdg.lib.operators.fhir_import import FhirCsvConfig

fhir_delete_config = FhirCsvConfig(
    fhir_url=fhir_url,
    keycloak_client_secret_name=keycloak_client_secret_name,
    keycloak_url=keycloak_url,
    aws_endpoint=aws_endpoint,
    aws_credentials_secret_name=aws_secret_name,
    aws_credentials_secret_access_key=aws_secret_access_key,
    aws_credentials_secret_secret_key=aws_secret_secret_key,
    clinical_data_bucket_name=clinical_data_bucket,
    file_import_bucket_name=file_import_bucket,
    id_service_url=Variable.get("id_service_url"),
    kube_config=kube_config,
    image=Variable.get("fhir_import_image"),
).args("bio/ferlab/cqdg/etl/FhirDelete")

with DAG(
    dag_id="etl-fhir-delete",
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    params={
        "studyVersion": Param("1", type="string"),
        "study_code": Param("study1", type="string"),
        "dryRun": Param("true", enum=["true", "false"], type="string"),
    },
) as dag:

    def study_version() -> str:
        return "{{ params.studyVersion }}"

    def dry_run() -> str:
        return "{{ params.dryRun }}"

    # FhirDelete expects: <version> <studyId> [dryRun]
    # It deletes every resource tagged `study:<study_code>` AND `study_version:<studyVersion>`,
    # i.e. the resources of that specific version of the study.
    # When dryRun is 'true' nothing is deleted; the resources that would be deleted are only logged.
    (
        fhir_delete_config.args(study_version(), study_code, dry_run()).operator(
            task_id="fhir_delete",
            name="etl-fhir-delete",
        )
    )
