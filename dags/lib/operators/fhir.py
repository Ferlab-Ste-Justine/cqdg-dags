from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from lib import config


class FhirOperator(KubernetesPodOperator):
    def __init__(
        self,
        k8s_context: str,
        color: str = '',
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
        self.color = color

    def execute(self, **kwargs):

        self.image_pull_secrets = [
            k8s.V1LocalObjectReference(
                name='images-registry-credentials',
            ),
        ]
        self.env_vars = [
            k8s.V1EnvVar(
                name='FHIR_URL',
                value='http://fhir-server:8080/fhir',
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
        ]

        super().execute(**kwargs)
