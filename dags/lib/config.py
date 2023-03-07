import kubernetes
from airflow.exceptions import AirflowConfigException
from airflow.models import Variable


class Env:
    QA = 'qa'
    DEV = 'dev'
    PROD = 'prd'


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
show_test_dags = Variable.get('show_test_dags', None) == 'yes'

fhavro_export_image = 'ferlabcrsj/fhavro-export:6cfd2da11bc98ba2017d5093606592b2a3de9b34-1678214674'
spark_image = 'ferlabcrsj/spark:3.3.1'
arranger_image = 'ferlabcrsj/cqdg-api-arranger:1.1.3'
spark_service_account = 'spark'
cqdg_fhir_import = 'ferlabcrsj/cqdg-fhir-import'
jar_version = 'v1.1.12'


if env == Env.QA:
    es_url = 'http://elasticsearch-workers'
    spark_index_jar = f'https://github.com/Ferlab-Ste-Justine/etl-cqdg-portal/releases/download/{jar_version}/index-task.jar'
    spark_publish_jar = f'https://github.com/Ferlab-Ste-Justine/etl-cqdg-portal/releases/download/{jar_version}/publish-task.jar'
    fhir_url = 'https://fhir.qa.cqdg.ferlab.bio'
    keycloak_url = 'https://keycloak-http'
    ca_certificates = 'ingress-ca-certificate'
    minio_certificate = 'minio-ca-certificate'
elif env == Env.DEV:
    es_url = 'http://elasticsearch-workers'
    spark_index_jar = f'https://github.com/Ferlab-Ste-Justine/etl-cqdg-portal/releases/download/{jar_version}/index-task.jar'
    spark_publish_jar = f'https://github.com/Ferlab-Ste-Justine/etl-cqdg-portal/releases/download/{jar_version}/publish-task.jar'
    fhir_url = 'https://fhir.qa.cqdg.ferlab.bio'
    keycloak_url = 'https://keycloak-http'
    ca_certificates = 'ingress-ca-certificate'
    minio_certificate = 'minio-ca-certificate'
elif env == Env.PROD:
    es_url = 'https://elasticsearch-workers'
    spark_index_jar = f'https://github.com/Ferlab-Ste-Justine/etl-cqdg-portal/releases/download/{jar_version}/index-task.jar'
    spark_publish_jar = f'https://github.com/Ferlab-Ste-Justine/etl-cqdg-portal/releases/download/{jar_version}/publish-task.jar'
    fhir_url = 'https://fhir.qa.cqdg.ferlab.bio'
    keycloak_url = 'https://keycloak-http'
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
