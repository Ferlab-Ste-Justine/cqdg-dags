from dataclasses import dataclass
from cqdg.lib.operators.base_kubernetes import BaseKubernetesOperator, BaseConfig, required
from kubernetes.client import models as k8s
from typing import Optional, Type


class MetadataOperator(BaseKubernetesOperator):

    def __init__(
            self,
            aws_endpoint: str,
            clinical_data_bucket: str,
            import_files_bucket: str,
            aws_credentials_secret_name: Optional[str] = None,
            aws_credentials_secret_access_key: str = 'access',
            aws_credentials_secret_secret_key: str = 'secret',
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.aws_endpoint = aws_endpoint
        self.aws_credentials_secret_name = aws_credentials_secret_name
        self.aws_credentials_secret_access_key = aws_credentials_secret_access_key
        self.aws_credentials_secret_secret_key = aws_credentials_secret_secret_key
        self.clinical_data_bucket = clinical_data_bucket
        self.import_files_bucket = import_files_bucket

    def execute(self, **kwargs):
        if self.aws_endpoint:
            self.env_vars.append(
                k8s.V1EnvVar(
                    name='S3_URL',
                    value=self.aws_endpoint,
                )
            )
        if self.aws_credentials_secret_name:
            self.env_vars.append(
                k8s.V1EnvVar(
                    name='S3_ACCESS_KEY',
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
                    name='S3_SECRET_KEY',
                    value_from=k8s.V1EnvVarSource(
                        secret_key_ref=k8s.V1SecretKeySelector(
                            name=self.aws_credentials_secret_name,
                            key=self.aws_credentials_secret_secret_key,
                        ),
                    ),
                )
            )

        self.env_vars.extend ([
            k8s.V1EnvVar(name='CLINICAL_DATA_BUCKET', value=self.clinical_data_bucket),
            k8s.V1EnvVar(name='IMPORT_FILES_BUCKET', value=self.import_files_bucket),
        ])

        self.cmds = ['python', 'produce_meta/main.py']

        super().execute(**kwargs)


@dataclass
class MetadataConfig(BaseConfig):
    aws_endpoint: Optional[str] = None
    aws_credentials_secret_name: Optional[str] = None
    aws_credentials_secret_access_key: str = 'access'
    aws_credentials_secret_secret_key: str = 'secret'
    clinical_data_bucket: str = required()
    import_files_bucket: str = required()

    def operator(self, class_to_instantiate: Type[MetadataOperator] = MetadataOperator, **kwargs) -> MetadataOperator:
        return super().build_operator(class_to_instantiate=class_to_instantiate, **kwargs)