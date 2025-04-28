from dataclasses import dataclass
from cqdg.lib.operators.base_kubernetes import BaseKubernetesOperator, BaseConfig, required
from kubernetes.client import models as k8s
from typing import Optional, Type

class FhavroOperator(BaseKubernetesOperator):

    def __init__(
        self,
        fhir_url: str,
        keycloak_url: str,        
        keycloak_client_secret_name: str,
        bucket_name:str,
        aws_endpoint:Optional[str] = None,
        aws_credentials_secret_name: Optional[str] = None,
        aws_credentials_secret_access_key: str = 'access',
        aws_credentials_secret_secret_key: str = 'secret',
        aws_region: str = 'us-east-1',
        aws_access_path_style: bool = True, 
        keycloak_client_secret_key: Optional[str] = 'client-secret',        
        **kwargs,
    ) -> None:
        super().__init__(
            **kwargs
        )
        self.fhir_url = fhir_url
        self.keycloak_url = keycloak_url
        self.bucket_name = bucket_name
        self.aws_access_path_style = aws_access_path_style
        self.aws_endpoint=aws_endpoint
        self.aws_credentials_secret_name = aws_credentials_secret_name
        self.aws_credentials_secret_access_key = aws_credentials_secret_access_key
        self.aws_credentials_secret_secret_key = aws_credentials_secret_secret_key
        self.aws_region = aws_region
        self.keycloak_client_secret_name = keycloak_client_secret_name
        self.keycloak_client_secret_key = keycloak_client_secret_key         

        

    def execute(self, **kwargs):
        if self.aws_endpoint:
            self.env_vars.append(
                k8s.V1EnvVar(
                    name='AWS_ENDPOINT',
                    value=self.aws_endpoint,
                )
            )
        if self.aws_credentials_secret_name:
            self.env_vars.append(
                k8s.V1EnvVar(
                    name='AWS_ACCESS_KEY_ID',
                    value_from=k8s.V1EnvVarSource(
                        secret_key_ref=k8s.V1SecretKeySelector(
                            name=self.aws_credentials_secret_name,
                            key=self.aws_credentials_secret_access_key,
                        ),
                    ),
                ),
            )
            self.env_vars.append(
                k8s.V1EnvVar(
                    name='AWS_SECRET_ACCESS_KEY',
                    value_from=k8s.V1EnvVarSource(
                        secret_key_ref=k8s.V1SecretKeySelector(
                            name=self.aws_credentials_secret_name,
                            key=self.aws_credentials_secret_secret_key,
                        ),
                    ),
                )
            )
        
        env_vars = [
            k8s.V1EnvVar(
                name='AWS_REGION',
                value=self.aws_region,
            ),
            k8s.V1EnvVar(
                name='KEYCLOAK_CLIENT_SECRET',
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name=self.keycloak_client_secret_name,
                        key=self.keycloak_client_secret_key,
                    ),
                ),
            ),
            k8s.V1EnvVar(
                name='KEYCLOAK_URL',
                value=self.keycloak_url,
            ),
            k8s.V1EnvVar(
                name='FHIR_URL',
                value=self.fhir_url,
            ),
            k8s.V1EnvVar(
                name = 'AWS_PATH_ACCESS_STYLE',
                value = 'true' if self.aws_access_path_style else 'false'
            ),
            k8s.V1EnvVar(
                name='BUCKET_NAME',
                value=self.bucket_name,
            )
        ]
        self.env_vars = [*self.env_vars, *env_vars]
        self.cmds=['java',
              '-cp',
              'fhavro-export.jar',
              'bio/ferlab/fhir/etl/FhavroExport'
              ]        

        super().execute(**kwargs)

@dataclass
class FhavroConfig(BaseConfig):
    fhir_url: str = required()
    keycloak_url: str = required()
    keycloak_client_secret_name: str = required()
    bucket_name:str = required()
    aws_endpoint:Optional[str] = None
    aws_credentials_secret_name: Optional[str] = None
    aws_credentials_secret_access_key: str = 'access'
    aws_credentials_secret_secret_key: str = 'secret'
    aws_region: str = 'us-east-1'
    aws_access_path_style: bool = True
    keycloak_client_secret_key: Optional[str] = 'client-secret'

    def operator(self, class_to_instantiate: Type[FhavroOperator] = FhavroOperator,**kwargs) -> FhavroOperator:
        return super().build_operator(class_to_instantiate=class_to_instantiate, **kwargs) 
            
