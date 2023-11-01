from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from lib import config
from lib.config import env

class FhavroOperator(KubernetesPodOperator):

    def __init__(
        self,
        k8s_context: str,
        **kwargs,
    ) -> None:
        super().__init__(
            is_delete_operator_pod=True,
            in_cluster=config.k8s_in_cluster(k8s_context),
            cluster_context=config.k8s_cluster_context(k8s_context),
            namespace=config.k8s_namespace,
            image=config.fhavro_export_image,
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
            ),
            k8s.V1EnvVar(
                name='AWS_REGION',
                value='us-east-1',
            ),
            k8s.V1EnvVar(
                name='KEYCLOAK_CLIENT_SECRET',
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name='keycloak-client-system-credentials',
                        key='client-secret',
                    ),
                ),
            ),
            k8s.V1EnvVar(
                name='KEYCLOAK_URL',
                value=config.keycloak_url,
            ),
            k8s.V1EnvVar(
                name='FHIR_URL',
                value=config.fhir_url,
            ),
            k8s.V1EnvVar(
                name='AWS_PATH_ACCESS_STYLE',
                value='true'
            ),
            k8s.V1EnvVar(
                name='BUCKET_NAME',
                value=f'cqdg-{env}-app-datalake',
            ),
        ]

        super().execute(**kwargs)
