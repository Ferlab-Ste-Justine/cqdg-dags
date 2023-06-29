from datetime import datetime

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client import models as k8s

from lib import config
from lib.config import env

with DAG(
        dag_id='test_bash',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
) as dag:
    script = f"""
        #!/bin/bash
        
        echo Setting MC alias to this minio: $AWS_ENDPOINT
        mc alias set myminio $AWS_ENDPOINT $AWS_ACCESS_KEY_ID $AWS_SECRET_ACCESS_KEY
        
        mkdir templates
        
        echo Downloading templates ...
        curl  https://raw.githubusercontent.com/Ferlab-Ste-Justine/etl-cqdg-portal/master/index-task/src/main/resources/templates/template_study_centric.json --output ./templates/template_study_centric.json
        curl  https://raw.githubusercontent.com/Ferlab-Ste-Justine/etl-cqdg-portal/master/index-task/src/main/resources/templates/template_file_centric.json --output ./templates/template_file_centric.json
        curl  https://raw.githubusercontent.com/Ferlab-Ste-Justine/etl-cqdg-portal/master/index-task/src/main/resources/templates/template_participant_centric.json --output ./templates/template_participant_centric.json
        curl  https://raw.githubusercontent.com/Ferlab-Ste-Justine/etl-cqdg-portal/master/index-task/src/main/resources/templates/template_biospecimen_centric.json --output ./templates/template_biospecimen_centric.json
        
        echo Copy templates ...
        mc cp ./templates/template_study_centric.json myminio/cqdg-{env}-app-clinical-data-service/templates/template_study_centric.json
        mc cp ./templates/template_file_centric.json myminio/cqdg-{env}-app-clinical-data-service/templates/template_file_centric.json
        mc cp ./templates/template_participant_centric.json myminio/cqdg-{env}-app-clinical-data-service/templates/template_participant_centric.json
        mc cp ./templates/template_biospecimen_centric.json myminio/cqdg-{env}-app-clinical-data-service/templates/template_biospecimen_centric.json     
    """

    test_bash = KubernetesPodOperator(
        task_id='es-templates-update',
        name='es-templates-update',
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
