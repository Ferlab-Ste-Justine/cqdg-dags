from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client import models as k8s

from cqdg.lib.config import datalake_bucket, kube_config, aws_endpoint, aws_secret_name, aws_secret_access_key, aws_secret_secret_key
from cqdg.lib.slack import Slack

script = f"""
    #!/bin/bash
    
    apk update; apk add -U curl
    
    curl https://dl.min.io/client/mc/release/linux-amd64/mc \
    --create-dirs \
    -o $HOME/minio-binaries/mc

    chmod +x $HOME/minio-binaries/mc
    export PATH=$PATH:$HOME/minio-binaries/
    
    echo Setting MC alias to this minio: $AWS_ENDPOINT
    mc alias set myminio $AWS_ENDPOINT $AWS_ACCESS_KEY_ID $AWS_SECRET_ACCESS_KEY
    
    mkdir templates
    
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
    mc cp ./templates/template_study_centric.json myminio/{datalake_bucket}/templates/template_study_centric.json
    mc cp ./templates/template_file_centric.json myminio/{datalake_bucket}/templates/template_file_centric.json
    mc cp ./templates/template_participant_centric.json myminio/{datalake_bucket}/templates/template_participant_centric.json
    mc cp ./templates/template_biospecimen_centric.json myminio/{datalake_bucket}/templates/template_biospecimen_centric.json 
        
    mc cp ./templates/template_variant_centric.json myminio/{datalake_bucket}/templates/template_variant_centric.json     
    mc cp ./templates/template_gene_centric.json myminio/{datalake_bucket}/templates/template_gene_centric.json     
    mc cp ./templates/template_variant_suggestions.json myminio/{datalake_bucket}/templates/template_variant_suggestions.json     
    mc cp ./templates/template_gene_suggestions.json myminio/{datalake_bucket}/templates/template_gene_suggestions.json     
"""

def es_templates_update():
    return KubernetesPodOperator(
        task_id='es_templates_update',
        name='es-templates-update',
        image="alpine:3.14",
        is_delete_operator_pod=True,
        cmds=["sh", "-cx"],
        arguments=[script],
        namespace=kube_config.namespace,
        env_vars=[
            k8s.V1EnvVar(
                name='AWS_ENDPOINT',
                value=aws_endpoint,
            ),
            k8s.V1EnvVar(
                name='AWS_ACCESS_KEY_ID',
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name=aws_secret_name,
                        key=aws_secret_access_key,
                    ),
                ),
            ),
            k8s.V1EnvVar(
                name='AWS_SECRET_ACCESS_KEY',
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name=aws_secret_name,
                        key=aws_secret_secret_key,
                    ),
                ),
            ), ],
        on_failure_callback=Slack.notify_task_failure
    )
