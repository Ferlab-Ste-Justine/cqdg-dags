from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.python import PythonOperator
from kubernetes import client, config
from airflow.models import Variable
import logging
from lib.config import env, es_url, Env, K8sContext

default_args = {
    "owner": "ihannache",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "ihannache@ferlab.bio"
}

namespace=Variable.get("kubernetes_namespace")

def _spark_task_check(ti):
    config.load_incluster_config()
    v1 = client.CoreV1Api()
    pod = v1.list_namespaced_pod(
        namespace=namespace,
        limit=3,
    )
    logging.info(pod)

with DAG("k8s_hello_world", start_date=days_ago(2),
    schedule_interval=None, catchup=False) as dag:
        task_hello_world = KubernetesPodOperator(
            namespace=namespace,
            image='alpine',
            cmds=["sh", "-c", "echo 'Hello WOrld!'"],
            name="say-hello",
            is_delete_operator_pod=False,
            task_id="say-hello",
            get_logs=True,
        )

        spark_task_check = PythonOperator(
            task_id="spark_task_check",
            python_callable=_spark_task_check,
        )


        task_hello_world >> spark_task_check