import kubernetes
import logging
from airflow.exceptions import AirflowFailException
from airflow.exceptions import AirflowSkipException
from kubernetes.client import models as k8s
from typing import Optional, List, Type
from collections import ChainMap
import copy
from lib.operators.base_kubernetes import BaseKubernetesOperator, BaseConfig
from typing_extensions import Self
from dataclasses import dataclass, field, asdict


class SparkOperator(BaseKubernetesOperator):
    template_fields = [*BaseKubernetesOperator.template_fields, 'spark_jar', 'spark_class', 'spark_configs',
                       'spark_packages', 'spark_exclude_packages']

    def __init__(
            self,
            spark_class: Optional[str] = None,
            spark_jar: Optional[str] = None,
            spark_configs: List[dict] = [],
            spark_config_volume: Optional[str] = None,
            spark_packages: List[str] = [],
            spark_exclude_packages: List[str] = [],
            is_skip: bool = False,
            is_skip_fail: bool = False,

            **kwargs
    ) -> None:
        super().__init__(
            **kwargs
        )
        self.spark_class = spark_class
        self.spark_jar = spark_jar
        self.spark_configs = spark_configs
        self.spark_config_volume = spark_config_volume
        self.spark_packages = spark_packages
        self.spark_exclude_packages = spark_exclude_packages
        self.is_skip = is_skip
        self.is_skip_fail = is_skip_fail

    def execute(self, **kwargs):

        if self.is_skip:
            raise AirflowSkipException()

        # Driver pod name
        self.env_vars.append(
            k8s.V1EnvVar(
                name='SPARK_CLIENT_POD_NAME',
                value_from=k8s.V1EnvVarSource(
                    field_ref=k8s.V1ObjectFieldSelector(
                        field_path='metadata.name',
                    ),
                )
            )
        )
        self.env_vars.append(
            k8s.V1EnvVar(
                name='SPARK_CLIENT_NAMESPACE',
                value_from=k8s.V1EnvVarSource(
                    field_ref=k8s.V1ObjectFieldSelector(
                        field_path='metadata.namespace',
                    ),
                )
            )
        )

        driver_pod_name_config = ['--conf', 'spark.kubernetes.driver.pod.name=$(SPARK_CLIENT_POD_NAME)-driver']

        # Build --conf attributes
        spark_config_reversed = reversed(self.spark_configs)
        merged_config = dict(ChainMap(*spark_config_reversed))

        if 'spark.kubernetes.driver.container.image' not in merged_config.keys() and 'spark.kubernetes.container.image' not in merged_config.keys():
            merged_config['spark.kubernetes.driver.container.image'] = self.image
        if 'spark.kubernetes.executor.container.image' not in merged_config.keys() and 'spark.kubernetes.container.image' not in merged_config.keys():
            merged_config['spark.kubernetes.executor.container.image'] = self.image
        if 'spark.master' not in merged_config.keys():
            merged_config['spark.master'] = 'k8s://https://kubernetes.default.svc'
        if 'spark.kubernetes.namespace' not in merged_config.keys():
            merged_config['spark.kubernetes.namespace'] = '$(SPARK_CLIENT_NAMESPACE)'
        if 'spark.kubernetes.authenticate.driver.serviceAccountName' not in merged_config.keys():
            merged_config['spark.kubernetes.authenticate.driver.serviceAccountName'] = self.service_account_name

        merged_config['spark.submit.deployMode'] = 'cluster'

        merged_config_attributes = [['--conf', f'{k}={v}'] for k, v in merged_config.items()]
        merged_config_attributes = sum(merged_config_attributes, [])  # flatten

        # Build --packages attribute
        spark_packages_attributes = ['--packages', ','.join(self.spark_packages)] if self.spark_packages else []

        # Build --exclude-packages attribute
        spark_exclude_packages_attributes = ['--exclude-packages', ','.join(self.spark_exclude_packages)] if self.spark_exclude_packages else []

        # CMD
        self.cmds = ['/opt/spark/bin/spark-submit']

        self.arguments = [*spark_packages_attributes, *spark_exclude_packages_attributes, *driver_pod_name_config, *merged_config_attributes, '--class',
                          self.spark_class, self.spark_jar, *self.arguments]

        # Mount additional config volume
        if self.spark_config_volume:
            self.volumes.append(
                k8s.V1Volume(
                    name='spark_config_volume',
                    config_map=k8s.V1ConfigMapVolumeSource(
                        name=self.spark_config_volume,
                    ),
                ),
            )
            self.volume_mounts.append(
                k8s.V1VolumeMount(
                    name='spark_config_volume',
                    mount_path=f'/opt/spark/conf',
                    read_only=True,
                ),
            )

        super().execute(**kwargs)

        k8s_client = kubernetes.client.CoreV1Api()

        # Get driver pod log and delete driver pod
        driver_pod = k8s_client.list_namespaced_pod(
            namespace=self.pod.metadata.namespace,
            field_selector=f'metadata.name={self.pod.metadata.name}-driver',
            limit=1,
        )

        if driver_pod.items:
            log = k8s_client.read_namespaced_pod_log(
                name=f'{self.pod.metadata.name}-driver',
                namespace=self.pod.metadata.namespace,
            )
            logging.info(f'Spark job log:\n{log}')
            k8s_client.delete_namespaced_pod(
                name=f'{self.pod.metadata.name}-driver',
                namespace=self.pod.metadata.namespace,
            )

        # Delete pod
        pod = k8s_client.list_namespaced_pod(
            namespace=self.pod.metadata.namespace,
            field_selector=f'metadata.name={self.pod.metadata.name}',
            limit=1,
        )
        if pod.items:
            k8s_client.delete_namespaced_pod(
                name=self.pod.metadata.name,
                namespace=self.pod.metadata.namespace,
            )

        # Fail task if driver pod failed
        if driver_pod.items[0].status.phase != 'Succeeded':
            if self.is_skip_fail:
                raise AirflowSkipException()
            else:
                raise AirflowFailException('Spark job failed')


@dataclass
class SparkOperatorConfig(BaseConfig):
    """"
    Configuration for SparkOperator
    :param spark_class: The main class of the Spark application
    :param spark_jar: The location of the Spark jar file
    :param spark_configs: List of Spark configuration
    :param spark_config_volume: Name of the ConfigMap to mount as volume
    :param spark_packages: List of Spark packages to install
    :param is_skip: Skip the operator if True
    :param is_skip_fail: Skip the operator if True and fail the task if the Spark job fails
    """
    spark_class: Optional[str] = None
    spark_jar: Optional[str] = None
    spark_configs: List[dict] = field(default_factory=list)
    spark_config_volume: Optional[str] = None
    spark_packages: List[str] = field(default_factory=list)
    spark_exclude_packages: List[str] = field(default_factory=list)
    is_skip: bool = False
    is_skip_fail: bool = False

    def add_spark_conf(self, *new_config) -> Self:
        c = copy.copy(self)
        c.spark_configs = [*self.spark_configs, *new_config]
        return c

    def add_package(self, new_package: str) -> Self:
        c = copy.copy(self)
        c.spark_packages = [*self.spark_packages, new_package]
        return c

    def add_packages(self, *new_packages) -> Self:
        c = copy.copy(self)
        c.spark_packages = [*self.spark_packages, *new_packages]
        return c

    def add_exclude_package(self, exclude_package: str) -> Self:
        c = copy.copy(self)
        c.exclude_packages = [*self.spark_exclude_packages, exclude_package]
        return c

    def add_exclude_packages(self, *exclude_packages) -> Self:
        c = copy.copy(self)
        c.exclude_packages = [*self.spark_exclude_packages, *exclude_packages]
        return c

    def skip(self) -> Self:
        c = copy.copy(self)
        c.is_skip = True
        return c

    def skip_fail(self) -> Self:
        c = copy.copy(self)
        c.is_skip_fail = True
        return c

    def skip_all(self) -> Self:
        c = copy.copy(self)
        c.is_skip = True
        c.is_skip_fail = True
        return c

    def delete(self) -> Self:
        c = copy.copy(self)
        c.is_delete = True
        return c

    def with_spark_class(self, spark_class: str) -> Self:
        c = copy.copy(self)
        c.spark_class = spark_class
        return c

    def with_spark_jar(self, spark_jar: str) -> Self:
        c = copy.copy(self)
        c.spark_jar = spark_jar
        return c

    def with_image(self, image: str) -> Self:
        c = copy.copy(self)
        c.image = image
        return c

    def with_spark_config_volume(self, spark_config_volume: str) -> Self:
        c = copy.copy(self)
        c.spark_config_volume = spark_config_volume
        return c

    def operator(self, class_to_instantiate: Type[SparkOperator] = SparkOperator, **kwargs) -> BaseKubernetesOperator:
        return super().build_operator(class_to_instantiate=class_to_instantiate, **kwargs)
