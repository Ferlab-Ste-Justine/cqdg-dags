from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from lib import config
from lib.config import env_url

class FhavroOperator(KubernetesPodOperator):

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
                name='AWS_ACCESS_KEY',
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name='s3-fhir-import-credentials',
                        key='S3_ACCESS_KEY',
                    ),
                ),
            ),
            k8s.V1EnvVar(
                name='AWS_SECRET_KEY',
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

        ]
        self.volumes = [
            k8s.V1Volume(
                name='minio-ca-certificate',
                config_map=k8s.V1ConfigMapVolumeSource(
                    name=config.minio_certificate,
                    default_mode=0o555,
                ),
            ),
        ]
        self.volume_mounts = [
            k8s.V1VolumeMount(
                name='minio-ca-certificate',
                mount_path='/opt/minio-ca',
                read_only=True,
            ),
        ]

        super().execute(**kwargs)
