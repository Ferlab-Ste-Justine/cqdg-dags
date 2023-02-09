from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from lib import config


class FhirCsvOperator(KubernetesPodOperator):

    template_fields = KubernetesPodOperator.template_fields

    def __init__(
        self,
        k8s_context: str,
        **kwargs,
    ) -> None:
        super().__init__(
            is_delete_operator_pod=True,
            in_cluster=config.k8s_in_cluster(k8s_context),
            config_file=config.k8s_config_file(k8s_context),
            cluster_context=config.k8s_cluster_context(k8s_context),
            namespace=config.k8s_namespace,
            image=config.cqdg_fhir_import,
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
                name='AWS_BUCKET_NAME',
                value='cqdg-prod-user-thibault-demalliard01',
            ),
            k8s.V1EnvVar(
                name='AWS_ENDPOINT',
                value='https://s3.ops.cqdg.ferlab.bio',
            ),
            k8s.V1EnvVar(
                name='FHIR_URL',
                value='http://fhir-server:8080/fhir',
            ),
            k8s.V1EnvVar(
                name='ID_SERVICE_HOST',
                value='http://id-service:5000',
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
                value='http://keycloak-http/auth/',
            ),
            k8s.V1EnvVar(
                name='NANUQ_ENDPOINT',
                value='https://ces.genomequebec.com/nanuqMPS/ws/getRunInfo',
            ),
            k8s.V1EnvVar(
                name='NANUQ_PASSWORD',
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name='nanuq-credentials',
                        key='nanuq-password',
                    ),
                ),
            ),
            k8s.V1EnvVar(
                name='NANUQ_USERNAME',
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name='nanuq-credentials',
                        key='nanuq-username',
                    ),
                ),
            ),
        ]
        self.cmds = ['java']

        super().execute(**kwargs)
