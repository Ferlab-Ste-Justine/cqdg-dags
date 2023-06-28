from airflow import DAG
from datetime import datetime
from lib.operators.run_import_minio import FileImportOperator
from lib.operators.fhavro import FhavroOperator
from lib.config import Env, K8sContext

with DAG(
        dag_id='test_bash',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
) as dag:

    toto = """
    #!/bin/bash
    echo Setting MC alias to this minio: $AWS_ENDPOINT
    """


    # test_bash = FileImportOperator(
    #     task_id='fhavro_export',
    #     name='etl-fhavro_export',
    #     k8s_context=K8sContext.DEFAULT,
    #     cmds=['echo Setting MC alias to this minio'],
    # )
    test_bash = FhavroOperator(
        task_id='fhavro_export',
        name='etl-fhavro_export',
        k8s_context=K8sContext.DEFAULT,
        cmds=["echo"],
    )
