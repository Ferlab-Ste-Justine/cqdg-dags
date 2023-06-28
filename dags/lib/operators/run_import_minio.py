from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from lib import config

class FileImportOperator(KubernetesPodOperator):
    def __init__(
            self,
            k8s_context: str,
            **kwargs,
    ) -> None:
        super().__init__(
            is_delete_operator_pod=False,
            in_cluster=config.k8s_in_cluster(k8s_context),
            cluster_context=config.k8s_cluster_context(k8s_context),
            namespace=config.k8s_namespace,
            image=config.fhavro_export_image,
            cmds=['util/es_mapping_import.sh $AWS_ENDPOINT $AWS_ACCESS_KEY_ID $AWS_SECRET_ACCESS_KEY'],
            **kwargs,
        )

    def execute(self, **kwargs):
        self.image_pull_secrets = [
            k8s.V1LocalObjectReference(
                name='images-registry-credentials',
            ),
        ]
        self.env_vars = [
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
            ),
            k8s.V1EnvVar(
                name='AWS_REGION',
                value='us-east-1',
            ),
        ]

        super().execute(**kwargs)
