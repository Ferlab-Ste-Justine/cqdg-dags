from airflow import DAG
from datetime import datetime
from lib import config
from lib.config import env, K8sContext
from kubernetes.client import models as k8s
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

with DAG(
        dag_id='es_templates_update',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        params={
        },
) as dag:

    script = f"""
    #!/bin/bash
    
    apk update
    apk add curl

    
    echo Setting MC alias to this minio: $AWS_ENDPOINT
    mc alias set myminio $AWS_ENDPOINT $AWS_ACCESS_KEY_ID $AWS_SECRET_ACCESS_KEY
    
    mkdir templates
    
    sleep 30m
    
    echo Downloading templates ...
    curl  https://raw.githubusercontent.com/Ferlab-Ste-Justine/etl-cqdg-portal/master/index-task/src/main/resources/templates/template_study_centric.json --output ./templates/template_study_centric.json
    curl  https://raw.githubusercontent.com/Ferlab-Ste-Justine/etl-cqdg-portal/master/index-task/src/main/resources/templates/template_file_centric.json --output ./templates/template_file_centric.json
    curl  https://raw.githubusercontent.com/Ferlab-Ste-Justine/etl-cqdg-portal/master/index-task/src/main/resources/templates/template_participant_centric.json --output ./templates/template_participant_centric.json
    curl  https://raw.githubusercontent.com/Ferlab-Ste-Justine/etl-cqdg-portal/master/index-task/src/main/resources/templates/template_biospecimen_centric.json --output ./templates/template_biospecimen_centric.json
    
    curl  https://raw.githubusercontent.com/Ferlab-Ste-Justine/etl-cqdg-portal/master/index-task/src/main/resources/templates/template_variant_centric.json --output ./templates/template_variant_centric.json
    curl  https://raw.githubusercontent.com/Ferlab-Ste-Justine/etl-cqdg-portal/master/index-task/src/main/resources/templates/template_gene_centric.json --output ./templates/template_gene_centric.json
    curl  https://raw.githubusercontent.com/Ferlab-Ste-Justine/etl-cqdg-portal/master/index-task/src/main/resources/templates/template_variant_suggestions.json --output ./templates/template_variant_suggestions.json
    curl  https://raw.githubusercontent.com/Ferlab-Ste-Justine/etl-cqdg-portal/master/index-task/src/main/resources/templates/template_gene_suggestions.json --output ./templates/template_gene_suggestions.json
    
    echo Copy templates ...
    mc cp https://raw.githubusercontent.com/Ferlab-Ste-Justine/etl-cqdg-portal/master/index-task/src/main/resources/templates/template_study_centric.json myminio/cqdg-{env}-app-datalake/templates/template_study_centric.json
    mc cp ./templates/template_file_centric.json myminio/cqdg-{env}-app-datalake/templates/template_file_centric.json
    mc cp ./templates/template_participant_centric.json myminio/cqdg-{env}-app-datalake/templates/template_participant_centric.json
    mc cp ./templates/template_biospecimen_centric.json myminio/cqdg-{env}-app-datalake/templates/template_biospecimen_centric.json 
        
    mc cp ./templates/template_variant_centric.json myminio/cqdg-{env}-app-datalake/templates/template_variant_centric.json     
    mc cp ./templates/template_gene_centric.json myminio/cqdg-{env}-app-datalake/templates/template_gene_centric.json     
    mc cp ./templates/template_variant_suggestions.json myminio/cqdg-{env}-app-datalake/templates/template_variant_suggestions.json     
    mc cp ./templates/template_gene_suggestions.json myminio/cqdg-{env}-app-datalake/templates/template_gene_suggestions.json     
"""

    es_templates_update = KubernetesPodOperator(
        task_id='es_templates_update',
        name='es-templates-update',
        image="alpine:3.18.4",
        # image="minio/mc",
        is_delete_operator_pod=True,
        cmds=["bash", "-cx"],
        arguments=[script],
        namespace=config.k8s_namespace,
        env_vars=[
            k8s.V1EnvVar(
                name='AWS_ENDPOINT',
                value='https://objets.juno.calculquebec.ca',
            ),
            k8s.V1EnvVar(
                name='AWS_ACCESS_KEY_ID',
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name='ceph-s3-credentials',
                        key='access',
                    ),
                ),
            ),
            k8s.V1EnvVar(
                name='AWS_SECRET_ACCESS_KEY',
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name='ceph-s3-credentials',
                        key='secret',
                    ),
                ),
            ), ]
    )
