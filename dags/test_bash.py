from airflow import DAG
from datetime import datetime
from lib.operators.run_import_minio import FileImportOperator
# from lib.operators.fhavro import
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from lib import config
from kubernetes.client import models as k8s

with DAG(
        dag_id='test_bash',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
) as dag:
    toto = """
    #!/bin/bash
    echo Setting MC alias to this minio:
    """

    test_bash = KubernetesPodOperator(
        task_id='fhavro_export',
        name='etl-fhavro_export',
        image="debian",
        is_delete_operator_pod=False,
        cmds=["bash", "-cx"],
        arguments=[toto],
        namespace=config.k8s_namespace,
        env_vars=[
            k8s.V1EnvVar(
                name='AWS_ENDPOINT',
                value='https://s3.ops.cqdg.ferlab.bio',
            ),
            k8s.V1EnvVar(
                name='AWS_ACCESS_KEY_ID',
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name='s3-fhir-import-credentials',
                        key='S3_ACCESS_KEY',
                    ),
                ),
            ),
            k8s.V1EnvVar(
                name='AWS_SECRET_ACCESS_KEY',
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name='s3-fhir-import-credentials',
                        key='S3_SECRET_KEY',
                    ),
                ),
            ), ]
    )
