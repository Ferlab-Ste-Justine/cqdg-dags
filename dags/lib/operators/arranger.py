from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from lib import config
# from lib.config import env

from dags.lib.config import Env


class ArrangerOperator(KubernetesPodOperator):

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
            image=config.arranger_image,
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
                name='ES_HOST',
                value="{0}:9200".format(config.es_url),
            ),
        ]

        # if env in [Env.PROD]:
        #     self.env_vars.append(
        #         k8s.V1EnvVar(
        #             name='NODE_ENV',
        #             value='production',
        #         )
        #     )
        #     self.env_vars.append(
        #         k8s.V1EnvVar(
        #             name='KEYCLOAK_CLIENT_SECRET',
        #             value_from=k8s.V1EnvVarSource(
        #                 secret_key_ref=k8s.V1SecretKeySelector(
        #                     name='arranger-keycloak-credentials',
        #                     key='SERVICE_ACCOUNT_CLIENT_SECRET',
        #                 ),
        #             ),
        #         )
        #     )
        # else:
        #     self.env_vars.append(
        #         k8s.V1EnvVar(
        #             name='NODE_ENV',
        #             value='qa',
        #         )
        #     )
        #     self.env_vars.append(
        #         k8s.V1EnvVar(
        #             name='KEYCLOAK_CLIENT_SECRET',
        #             value_from=k8s.V1EnvVarSource(
        #                 secret_key_ref=k8s.V1SecretKeySelector(
        #                     name='keycloak-client-system-credentials',
        #                     key='client-secret',
        #                 ),
        #             ),
        #         )
        #     )

        super().execute(**kwargs)
