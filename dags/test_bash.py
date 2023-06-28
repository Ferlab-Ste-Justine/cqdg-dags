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
    script = """
        #!/bin/bash
        
        curl https://dl.min.io/client/mc/release/linux-amd64/mc \
          --create-dirs \
          -o $HOME/minio-binaries/mc
        
        chmod +x $HOME/minio-binaries/mc
        export PATH=$PATH:$HOME/minio-binaries/
        
        echo Setting MC alias to this minio: $AWS_ENDPOINT
        echo Downloading templates ...
        mkdir templates
        curl  https://raw.githubusercontent.com/Ferlab-Ste-Justine/etl-cqdg-portal/master/index-task/src/main/resources/templates/template_study_centric.json --output ./templates/template_study_centric.json

        
    """

    test_bash = KubernetesPodOperator(
        task_id='fhavro_export',
        name='etl-fhavro_export',
        image="minio/mc",
        is_delete_operator_pod=False,
        cmds=["bash", "-cx"],
        arguments=[script],
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
