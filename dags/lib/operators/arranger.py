from kubernetes.client import models as k8s
from typing import Optional, List, Type
from dataclasses import dataclass
from lib.operators.base_kubernetes import BaseKubernetesOperator, BaseConfig, required

class ArrangerOperator(BaseKubernetesOperator):
    template_fields = [*BaseKubernetesOperator.template_fields, 'node_environment', 'es_url', 'spark_class', 
                       'es_port', 'es_cert_secret_name', 'es_credentials_secret_name', 'es_credentials_secret_key_username', 'es_credentials_secret_key_password',
                       'keycloak_client_secret_name', 'keycloak_client_secret_key'
                       ]

    def __init__(
        self,
        node_environment: str,
        es_url: str,            
        es_port: Optional[str] = '9200',
        es_cert_secret_name: Optional[str] = None,
        es_cert_file: Optional[str] = 'ca.crt',
        es_credentials_secret_name: Optional[str] = None,
        es_credentials_secret_key_username: Optional[str] = 'username',
        es_credentials_secret_key_password: Optional[str] = 'password',
        keycloak_client_secret_name: Optional[str] = None,
        keycloak_client_secret_key: Optional[str] = 'client-secret',
        **kwargs,
    ) -> None:
        super().__init__(
           **kwargs
        )
        self.node_environment=node_environment
        self.es_url=es_url
        self.es_port=es_port
        self.es_cert_secret_name=es_cert_secret_name
        self.es_cert_file=es_cert_file
        self.es_credentials_secret_name=es_credentials_secret_name
        self.es_credentials_secret_key_username=es_credentials_secret_key_username
        self.es_credentials_secret_key_password=es_credentials_secret_key_password
        self.keycloak_client_secret_name=keycloak_client_secret_name
        self.keycloak_client_secret_key=keycloak_client_secret_key        
        
    def execute(self, **kwargs):

        self.env_vars.append(
            k8s.V1EnvVar(
                name='NODE_ENV',
                value=self.node_environment,
            )
        )        
        self.env_vars.append(
            k8s.V1EnvVar(
                name='ES_HOST',
                value=f"{self.es_url}:{self.es_port}",
            ),            
        )

        if self.es_credentials_secret_name:
            self.env_vars.append(
                 k8s.V1EnvVar(
                    name='ES_USER',
                    value_from=k8s.V1EnvVarSource(
                        secret_key_ref = k8s.V1SecretKeySelector(
                        name=self.es_credentials_secret_name,
                        key=self.es_credentials_secret_key_username)
                    )
                )
            )
            self.env_vars.append(
                k8s.V1EnvVar(
                    name='ES_PASS',
                    value_from=k8s.V1EnvVarSource(
                        secret_key_ref=k8s.V1SecretKeySelector(
                        name=self.es_credentials_secret_name,
                        key=self.es_credentials_secret_key_password)
                    )
                )
            )

        if self.keycloak_client_secret_name:
            self.env_vars.append(
                k8s.V1EnvVar(
                    name='KEYCLOAK_CLIENT_SECRET',
                    value_from=k8s.V1EnvVarSource(
                        secret_key_ref=k8s.V1SecretKeySelector(
                            name=self.keycloak_client_secret_name,
                            key=self.keycloak_client_secret_key,
                        ),
                    ),
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
            self.env_vars.append(
                k8s.V1EnvVar(
                    name='NODE_EXTRA_CA_CERTS',
                    value=f'/opt/opensearch-ca/{self.es_cert_file}',
                )
            )                    

        self.cmds = ['node']
        super().execute(**kwargs)


@dataclass
class ArrangerConfig(BaseConfig):
    node_environment: str = required() # we need a default value because BaseConfig has some default fields. See https://stackoverflow.com/questions/51575931/class-inheritance-in-python-3-7-dataclasses
    es_url: Optional[str] = required()
    es_port: Optional[str] = '9200'
    es_cert_secret_name: Optional[str] = None
    es_cert_file: Optional[str] = 'ca.crt'
    es_credentials_secret_name: Optional[str] = None
    es_credentials_secret_key_username: Optional[str] = 'username'
    es_credentials_secret_key_password: Optional[str] = 'password'
    keycloak_client_secret_name: Optional[str] = None
    keycloak_client_secret_key: Optional[str] = 'client-secret'
    
    def operator(self, class_to_instantiate: Type[ArrangerOperator] = ArrangerOperator,**kwargs) -> ArrangerOperator:
        return super().build_operator(class_to_instantiate=class_to_instantiate, **kwargs) 