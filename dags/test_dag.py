from airflow import DAG
from airflow.models.param import Param
from datetime import datetime
from lib import config
from lib.config import env, Env, K8sContext
from lib.operators.spark import SparkOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.models import Variable
with DAG(
        dag_id='test_dag',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None
) as dag:
    run_this = KubernetesPodOperator(
    task_id="test_error_message",
    image=Variable.get("hello"),
    cmds=["/bin/sh"],
    arguments=["-c", "echo : {{ var.value.hello}}"],
    name="test-config-map"
)

