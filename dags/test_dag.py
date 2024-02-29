from airflow import DAG
from airflow.models.param import Param
from datetime import datetime
from lib import config
from lib.config import env, Env, K8sContext
from lib.operators.spark import SparkOperator
from airflow.operators.bash import BashOperator
with DAG(
        dag_id='test_dag',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None
) as dag:
    run_this = BashOperator(
        task_id="run_after_loop",
        bash_command="echo {{ var.value.hello}}",
    )
