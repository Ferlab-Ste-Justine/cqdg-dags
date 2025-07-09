from datetime import datetime
from airflow import DAG
from airflow.models import Param, Variable

from cqdg.lib.config import aws_endpoint, aws_secret_name, aws_secret_secret_key, kube_config, study_code, study_id
from cqdg.lib.operators.metadata_operator import MetadataConfig

metadata_config = MetadataConfig(
    aws_endpoint=aws_endpoint,
    aws_credentials_secret_name=aws_secret_name,
    aws_credentials_secret_secret_key=aws_secret_secret_key,
    clinical_data_bucket=Variable.get('CLINICAL_DATA_BUCKET'),
    import_files_bucket=Variable.get('FILE_IMPORT_BUCKET'),
    kube_config=kube_config,
    image=Variable.get('data_management_metadata_image'),
)

with DAG(
        dag_id='metadata',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        params={
            'study_code': Param('study1', type='string'),
            'study_id': Param('18', type='string'),
            'study_version': Param('31', type='string'),
        },
) as dag:
    def study_clin_data_version() -> str:
        return '{{ params.study_version }}'

    metadata_create = (metadata_config.args(
        '--study_code', '{{ params.study_code }}',
        '--study_id', '{{ params.study_id }}',
        '--study_version', '{{ params.study_version }}')
    .operator(
        task_id='metadata_create',
        name='metadata-create',
        is_delete_operator_pod=False,
    ))