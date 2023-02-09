import kubernetes
from airflow.exceptions import AirflowConfigException
from airflow.models import Variable


class Env:
    QA = 'qa'
    DEV = 'dev'
    PROD = 'prod'


class K8sContext:
    DEFAULT = 'default'
    ETL = 'etl'


env = Variable.get('environment')
k8s_namespace = Variable.get('kubernetes_namespace')
k8s_context = {
    K8sContext.DEFAULT: Variable.get('kubernetes_context_default', None),
    K8sContext.ETL: Variable.get('kubernetes_context_etl', None),
}
base_url = Variable.get('base_url', None)
s3_conn_id = Variable.get('s3_conn_id', None)
slack_hook_url = Variable.get('slack_hook_url', None)
show_test_dags = Variable.get('show_test_dags', None) == 'yes'

fhavro_export_image = 'ferlabcrsj/fhavro-export'
spark_image = 'ferlabcrsj/spark:3.3.1'
spark_service_account = 'spark'

if env == Env.QA:
    es_url = 'http://elasticsearch:9200'
    spark_index_jar = 'https://github.com/Ferlab-Ste-Justine/etl-cqdg-portal/releases/download/v1.0.1/index-task.jar'
    ca_certificates = 'ingress-ca-certificate'
    minio_certificate = 'minio-ca-certificate'
elif env == Env.DEV:
    es_url = 'http://elasticsearch:9200'
    spark_index_jar = 'https://github.com/Ferlab-Ste-Justine/etl-cqdg-portal/releases/download/v1.0.1/index-task.jar'
    ca_certificates = 'ingress-ca-certificate'
    minio_certificate = 'minio-ca-certificate'
elif env == Env.PROD:
    es_url = 'http://elasticsearch:9200'
    spark_index_jar = 'https://github.com/Ferlab-Ste-Justine/etl-cqdg-portal/releases/download/v1.0.1/index-task.jar'
    ca_certificates = 'ingress-ca-certificate'
    minio_certificate = 'minio-ca-certificate'
else:
    raise AirflowConfigException(f'Unexpected environment "{env}"')


def env_url(prefix: str = '') -> str:
    return f'{prefix}{env}' if env in [Env.QA, Env.DEV] else ''


def k8s_in_cluster(context: str) -> bool:
    return not k8s_context[context]


def k8s_config_file(context: str) -> str:
    return None if not k8s_context[context] else '~/.kube/config'


def k8s_cluster_context(context: str) -> str:
    return k8s_context[context]


def k8s_load_config(context: str) -> None:
    if not k8s_context[context]:
        kubernetes.config.load_incluster_config()
    else:
        kubernetes.config.load_kube_config(
            config_file=k8s_config_file(context),
            context=k8s_context[context],
        )
