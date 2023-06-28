from airflow import DAG
from datetime import datetime
from lib.operators.run_import_minio import FileImportOperator
# from lib.operators.fhavro import
from lib.config import env, Env, K8sContext
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from lib import config

with DAG(
        dag_id='test_bash',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
) as dag:

    toto = """
    #!/bin/bash
    echo Setting MC alias to this minio:
    """


    test_bash = FileImportOperator(
        task_id='fhavro_export',
        name='etl-fhavro_export',
        image="debian",
        is_delete_operator_pod=False,
        k8s_context=K8sContext.DEFAULT,
        cmds=["bash", "-cx"],
        arguments=[toto],
    )
