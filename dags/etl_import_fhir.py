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
        'prefixFiles': Param('CAG', type='string'),
        'bucket': Param('cqdg-qa-app-clinical-data-service', type='string'),
        'version': Param('7', type='string'),
        'release': Param('13', type='string'),
        'study': Param('cag', type='string'),
    },
) as dag:

    def prefix() -> str:
        return '{{ params.prefix }}'

    def prefix_files() -> str:
        return '{{ params.prefixFiles }}'

    def bucket() -> str:
        return '{{ params.bucket }}'

    def version() -> str:
        return '{{ params.version }}'

    def release() -> str:
        return '{{ params.release }}'

    def study() -> str:
        return '{{ params.study }}'

    csv_import = FhirCsvOperator(
        task_id='fhir_import',
        name='etl-fhir_import',
        k8s_context=K8sContext.DEFAULT,
        arguments=["-cp", "cqdg-fhir-import.jar", "bio/ferlab/cqdg/etl/FhirImport",
                   prefix(), prefix_files(), bucket(), version(), release(), study(), "true", ""],
    )
