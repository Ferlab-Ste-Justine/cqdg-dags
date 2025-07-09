from kubernetes.client import models as k8s
from typing import Optional, Type
from dataclasses import dataclass
from cqdg.lib.operators.base_kubernetes import BaseKubernetesOperator, BaseConfig, required


class PublishOperator(BaseKubernetesOperator):
    def __init__(
            self,
            es_url: str,
            es_port: Optional[str] = '9200',
            es_cert_secret_name: Optional[str] = None,
            es_cert_file: Optional[str] = 'ca.crt',
            es_credentials_secret_name: Optional[str] = None,
            es_credentials_secret_key_username: Optional[str] = 'username',
            es_credentials_secret_key_password: Optional[str] = 'password',
            **kwargs,
    ) -> None:
        super().__init__(
            **kwargs
        )
        self.es_url = es_url
        self.es_port = es_port
        self.es_cert_secret_name = es_cert_secret_name
        self.es_cert_file = es_cert_file
        self.es_credentials_secret_name = es_credentials_secret_name
        self.es_credentials_secret_key_username = es_credentials_secret_key_username
        self.es_credentials_secret_key_password = es_credentials_secret_key_password

    def execute(self, **kwargs):

        self.env_vars.append(
            k8s.V1EnvVar(
                name='ES_HOST',
                value=f"{self.es_url}:{self.es_port}",
            ),
        )

        if self.es_credentials_secret_name:
            self.env_vars.append(
                k8s.V1EnvVar(
                    name='ES_USERNAME',
                    value_from=k8s.V1EnvVarSource(
                        secret_key_ref=k8s.V1SecretKeySelector(
                            name=self.es_credentials_secret_name,
                            key=self.es_credentials_secret_key_username)
                    )
                )
            )
            self.env_vars.append(
                k8s.V1EnvVar(
                    name='ES_PASSWORD',
                    value_from=k8s.V1EnvVarSource(
                        secret_key_ref=k8s.V1SecretKeySelector(
                            name=self.es_credentials_secret_name,
                            key=self.es_credentials_secret_key_password)
                    )
                )
            )

        if self.es_cert_secret_name:
            self.volumes.append(
                k8s.V1Volume(
                    name=self.es_cert_secret_name,
                    secret=k8s.V1SecretVolumeSource(
                        secret_name='opensearch-ca-certificate',
                        default_mode=0o555
                    ),
                ),
            )
            self.volume_mounts.append(
                k8s.V1VolumeMount(
                    name=self.es_cert_secret_name,
                    mount_path='/opt/opensearch-ca',
                    read_only=True,
                ),
            )

        self.cmds = ['java',
                     '-cp',
                     'publish-task.jar',
                     'bio.ferlab.fhir.etl.PublishTask'
                     ]

        super().execute(**kwargs)


@dataclass
class PublishConfig(BaseConfig):
    es_url: Optional[str] = required()
    es_port: Optional[str] = '9200'
    es_cert_secret_name: Optional[str] = None
    es_cert_file: Optional[str] = 'ca.crt'
    es_credentials_secret_name: Optional[str] = None
    es_credentials_secret_key_username: Optional[str] = 'username'
    es_credentials_secret_key_password: Optional[str] = 'password'

    def operator(self, class_to_instantiate: Type[PublishOperator] = PublishOperator, **kwargs) -> PublishOperator:
        return super().build_operator(class_to_instantiate=class_to_instantiate, **kwargs)
