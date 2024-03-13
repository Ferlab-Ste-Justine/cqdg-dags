from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from typing import Optional, List, Type, TypeVar
from typing_extensions import Self
from kubernetes.client import models as k8s
import copy
from dataclasses import dataclass, field, asdict
@dataclass
class KubeConfig:
    in_cluster: bool = True,
    cluster_context: Optional[str] = None
    namespace: Optional[str] = None
    service_account_name: Optional[str] = None
    image_pull_secrets_name: Optional[str] = None    

class BaseKubernetesOperator(KubernetesPodOperator):
    template_fields = [*KubernetesPodOperator.template_fields, 'image_pull_secrets_name']
    def __init__(
            self,
            image_pull_secrets_name: Optional[str] = None,
            **kwargs
    ) -> None:
        super().__init__(
            **kwargs
        )   
        self.image_pull_secrets_name=image_pull_secrets_name
        
    def execute(self, **kwargs):

        if self.image_pull_secrets_name:
            self.image_pull_secrets = [
                k8s.V1LocalObjectReference(
                    name = self.image_pull_secrets_name,
                ),
            ]  
        super().execute(**kwargs)

T = TypeVar("T")

def required() -> T:
    f: T

    def factory() -> T:
        # mypy treats a Field as a T, even though it has attributes like .name, .default, etc
        field_name = f.name  # type: ignore[attr-defined]
        raise ValueError(f"field '{field_name}' required")

    f = field(default_factory=factory)
    return f

@dataclass          
class BaseConfig:
    kube_config: KubeConfig
    is_delete_operator_pod: bool = False
    image: Optional[str] = None
    arguments: List[str] = field(default_factory=list)    

    def args(self, *new_args) -> Self:
        c = copy.copy(self)
        c.arguments = [*self.arguments, *new_args]
        return c        
    
    def prepend_args(self, *new_args) -> Self:
        c = copy.copy(self)
        c.arguments = [*new_args, *self.arguments]
        return c            
    
    def build_operator(self, class_to_instantiate: Type[BaseKubernetesOperator], **kwargs) -> BaseKubernetesOperator:
        this_params = asdict(self)
        this_params.pop('kube_config', None)
        params = {**this_params, **kwargs}
        return class_to_instantiate(
            in_cluster = self.kube_config.in_cluster,
            cluster_context = self.kube_config.cluster_context,
            namespace = self.kube_config.namespace,
            service_account_name = self.kube_config.service_account_name,          
            **params
        )          
    
    def with_image(self, new_image) -> Self:
        c = copy.copy(self)
        c.image = new_image
        return c