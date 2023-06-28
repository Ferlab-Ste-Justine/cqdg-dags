from airflow import DAG
from datetime import datetime
from lib.operators.run_import_minio import FileImportOperator
from lib.config import Env, K8sContext

with DAG(
        dag_id='test_bash',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
) as dag:
    test_bash = FileImportOperator(
        task_id='fhavro_export',
        name='etl-fhavro_export',
        k8s_context=K8sContext.DEFAULT,
        cmds=[f'util/es_mapping_import.sh $AWS_ENDPOINT $AWS_ACCESS_KEY_ID $AWS_SECRET_ACCESS_KEY {Env}'],
    )
