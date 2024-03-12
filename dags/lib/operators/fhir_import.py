from dataclasses import dataclass
from lib.operators.base_kubernetes import BaseKubernetesOperator, BaseConfig, required
from kubernetes.client import models as k8s
from typing import Optional, Type

class FhirCsvOperator(BaseKubernetesOperator):

    def __init__(
            self,
            fhir_url: str,
            keycloak_url: str,        
            id_service_url: str,        
            keycloak_client_secret_name: str,
            clinical_data_bucket_name:str,
            file_import_bucket_name:str,
            aws_endpoint:Optional[str] = None,
            aws_credentials_secret_name: Optional[str] = None,
            aws_credentials_secret_access_key: str = 'access',
            aws_credentials_secret_secret_key: str = 'secret',
            aws_access_path_style: bool = True,
            keycloak_client_secret_key: Optional[str] = 'client-secret',
            **kwargs,
    ) -> None:
            super().__init__(**kwargs)
            self.fhir_url = fhir_url
            self.keycloak_url = keycloak_url
            self.id_service_url = id_service_url
            self.clinical_data_bucket_name = clinical_data_bucket_name
            self.file_import_bucket_name = file_import_bucket_name
            self.aws_access_path_style = aws_access_path_style
            self.aws_endpoint=aws_endpoint
            self.aws_credentials_secret_name = aws_credentials_secret_name
            self.aws_credentials_secret_access_key = aws_credentials_secret_access_key
            self.aws_credentials_secret_secret_key = aws_credentials_secret_secret_key
            self.keycloak_client_secret_name = keycloak_client_secret_name
            self.keycloak_client_secret_key = keycloak_client_secret_key 
    
    def execute(self, **kwargs):

        if self.config.aws_endpoint:
            self.env_vars.append(
                k8s.V1EnvVar(
                    name='AWS_ENDPOINT',
                    value=self.config.aws_endpoint,
                )
            )
        if self.config.aws_credentials_secret_name:
            self.env_vars.append(
                k8s.V1EnvVar(
                    name='AWS_ACCESS_KEY_ID',
                    value_from=k8s.V1EnvVarSource(
                        secret_key_ref=k8s.V1SecretKeySelector(
                            name=self.config.aws_credentials_secret_name,
                            key=self.config.aws_credentials_secret_access_key,
                        ),
                    ),
                ),
            )
            self.env_vars.append(
                k8s.V1EnvVar(
                    name='AWS_SECRET_ACCESS_KEY',
                    value_from=k8s.V1EnvVarSource(
                        secret_key_ref=k8s.V1SecretKeySelector(
                            name=self.config.aws_credentials_secret_name,
                            key=self.config.aws_credentials_secret_secret_key,
                        ),
                    ),
                )
            )
        self.env_vars = [
            *self.env_vars,
            k8s.V1EnvVar(
                name='S3_CLINICAL_DATA_BUCKET_NAME',
                value=self.config.clinical_data_bucket_name,
            ),

            k8s.V1EnvVar(
                name='FHIR_URL',
                value=self.config.fhir_url,
            ),
            k8s.V1EnvVar(
                name='ID_SERVICE_HOST',
                value=self.config.id_service_url,
            ),
            k8s.V1EnvVar(
                name='S3_FILE_IMPORT_BUCKET',
                value=self.config.file_import_bucket_name,
            ),
            k8s.V1EnvVar(
                name='KEYCLOAK_CLIENT_SECRET',
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name=self.config.keycloak_client_secret_name,
                        key=self.config.keycloak_client_secret_key,
                    ),
                ),
            ),
            k8s.V1EnvVar(
                name='KEYCLOAK_URL',
                value=self.config.keycloak_url,
            ),
        ]
        self.cmds = ['java', '-cp', 'cqdg-fhir-import.jar']

        super().execute(**kwargs)
        
@dataclass
class FhirCsvConfig(BaseConfig):
    fhir_url: str = required()
    keycloak_url: str = required()
    id_service_url: str = required()
    keycloak_client_secret_name: str = required()
    clinical_data_bucket_name:str = required()
    file_import_bucket_name:str = required()
    aws_endpoint:Optional[str] = None
    aws_credentials_secret_name: Optional[str] = None
    aws_credentials_secret_access_key: str = 'access'
    aws_credentials_secret_secret_key: str = 'secret'
    aws_access_path_style: bool = True
    keycloak_client_secret_key: Optional[str] = 'client-secret'      

    def operator(self, class_to_instantiate: Type[FhirCsvOperator] = FhirCsvOperator,**kwargs) -> FhirCsvOperator:
        return super().build_operator(class_to_instantiate=class_to_instantiate, **kwargs)     