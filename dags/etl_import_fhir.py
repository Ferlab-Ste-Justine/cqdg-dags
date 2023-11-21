from datetime import datetime

from airflow import DAG
from airflow.models.param import Param

from lib.config import K8sContext
from lib.operators.fhir_import import FhirCsvOperator

with DAG(
    dag_id='etl_import_fhir',
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    params={
        'prefix': Param('prefix', type='string'),
        'bucket': Param('cqdg-qa-app-clinical-data-service', type='string'),
        'studyId': Param('7', type='string'),
        'studyVersion': Param('13', type='string'),
        'study': Param('cag', type='string'),
        'project': Param('jmichaud', type='string'),
        'is_restricted': Param('', enum=['', 'true', 'false']),
    },
) as dag:

    def prefix() -> str:
        return '{{ params.prefix }}'

    def bucket() -> str:
        return '{{ params.bucket }}'

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

    csv_import = FhirCsvOperator(
        task_id='fhir_import',
        name='etl-fhir_import',
        k8s_context=K8sContext.DEFAULT,
        arguments=["-cp", "cqdg-fhir-import.jar", "bio/ferlab/cqdg/etl/FhirImport",
                   prefix(), bucket(), study_clin_data_id(), study_clin_data_version(), study(), project(), "true", is_restricted()],
    )
