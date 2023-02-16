import time

import kubernetes
import logging
from airflow.exceptions import AirflowFailException
from airflow.exceptions import AirflowSkipException
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from lib import config
from lib.config import env
from typing import List


class SparkOperator(KubernetesPodOperator):

    def __init__(
        self,
        k8s_context: str,
        spark_jar: str,
        spark_class: str,
        spark_config: str = '',
        spark_secret: str = '',
        skip_env: List[str] = [],
        skip_fail_env: List[str] = [],
        **kwargs,
    ) -> None:
        super().__init__(
            is_delete_operator_pod=False,
            in_cluster=config.k8s_in_cluster(k8s_context),
            # config_file=config.k8s_config_file(k8s_context),
            cluster_context=config.k8s_cluster_context(k8s_context),
            namespace=config.k8s_namespace,
            service_account_name=config.spark_service_account,
            image=config.spark_image,
            **kwargs,
        )
        self.k8s_context = k8s_context
        self.spark_class = spark_class
        self.spark_jar = spark_jar
        self.spark_config = spark_config
        self.spark_secret = spark_secret
        self.skip_env = skip_env
        self.skip_fail_env = skip_fail_env

    def execute(self, **kwargs):
        if env in self.skip_env:
            raise AirflowSkipException()

        self.cmds = ['/opt/client-entrypoint.sh']
        self.image_pull_policy = 'Always'
        self.image_pull_secrets = [
            k8s.V1LocalObjectReference(
                name='images-registry-credentials',
            ),
        ]
        self.env_vars = [
            k8s.V1EnvVar(
                name='SPARK_CLIENT_POD_NAME',
                value_from=k8s.V1EnvVarSource(
                    field_ref=k8s.V1ObjectFieldSelector(
                        field_path='metadata.name',
                    ),
                ),
            ),
            k8s.V1EnvVar(
                name='AWS_ENDPOINT',
                value='https://s3.ops.cqdg.ferlab.bio',
            ),
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
                name='SPARK_JAR',
                value='https://github.com/Ferlab-Ste-Justine/etl-cqdg-portal/releases/download/v1.1.4/index-task.jar',
            ),
            k8s.V1EnvVar(
                name='SPARK_CLASS',
                value='bio.ferlab.fhir.etl.IndexTask',
            ),
        ]
        self.volumes = [
            k8s.V1Volume(
                name='spark-defaults',
                config_map=k8s.V1ConfigMapVolumeSource(
                    name='spark-defaults',
                ),
            ),
            k8s.V1Volume(
                name='spark-s3-credentials',
                secret=k8s.V1SecretVolumeSource(
                    secret_name='spark-s3-credentials',
                ),
            ),
        ]
        self.volume_mounts = [
            k8s.V1VolumeMount(
                name='spark-defaults',
                mount_path='/opt/spark-configs/defaults',
                read_only=True,
            ),
            k8s.V1VolumeMount(
                name='spark-s3-credentials',
                mount_path='/opt/spark-configs/s3-credentials',
                read_only=True,
            ),
        ]

        if self.spark_config:
            self.volumes.append(
                k8s.V1Volume(
                    name=self.spark_config,
                    config_map=k8s.V1ConfigMapVolumeSource(
                        name=self.spark_config,
                    ),
                ),
            )
            self.volume_mounts.append(
                k8s.V1VolumeMount(
                    name=self.spark_config,
                    mount_path=f'/opt/spark-configs/{self.spark_config}',
                    read_only=True,
                ),
            )
        if self.spark_secret:
            self.volumes.append(
                k8s.V1Volume(
                    name=self.spark_secret,
                    secret=k8s.V1SecretVolumeSource(
                        secret_name=self.spark_secret,
                    ),
                ),
            )
            self.volume_mounts.append(
                k8s.V1VolumeMount(
                    name=self.spark_secret,
                    mount_path=f'/opt/spark-configs/{self.spark_secret}',
                    read_only=True,
                ),
            )

        super().execute(**kwargs)

        config.k8s_load_config(self.k8s_context)
        k8s_client = kubernetes.client.CoreV1Api()

        # Get driver pod log and delete driver pod
        # driver_pod = k8s_client.list_namespaced_pod(
        #     namespace=self.pod.metadata.namespace,
        #     field_selector=f'metadata.name={self.pod.metadata.name}-driver',
        #     limit=1,
        # )
        # if driver_pod.items:
        #     log = k8s_client.read_namespaced_pod_log(
        #         name=f'{self.pod.metadata.name}-driver',
        #         namespace=self.pod.metadata.namespace,
        #     )
        #     logging.info(f'Spark job log:\n{log}')
        #     k8s_client.delete_namespaced_pod(
        #         name=f'{self.pod.metadata.name}-driver',
        #         namespace=self.pod.metadata.namespace,
        #     )

        # Delete pod
        # pod = k8s_client.list_namespaced_pod(
        #     namespace=self.pod.metadata.namespace,
        #     field_selector=f'metadata.name={self.pod.metadata.name}',
        #     limit=1,
        # )
        # if pod.items:
        #     k8s_client.delete_namespaced_pod(
        #         name=self.pod.metadata.name,
        #         namespace=self.pod.metadata.namespace,
        #     )

        # Fail task if driver pod failed
        # if driver_pod.items[0].status.phase != 'Succeeded':
        #     if env in self.skip_fail_env:
        #         raise AirflowSkipException()
        #     else:
        #         raise AirflowFailException('Spark job failed')
