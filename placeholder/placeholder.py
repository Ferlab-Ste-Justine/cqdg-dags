"""
Placeholder DAG to validate jobs are running properly
"""
from datetime import timedelta
import time
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def sleep_for(random_base):
    """This is a function that will run within the DAG execution"""
    time.sleep(random_base)

with DAG(
    'placeholder',
    default_args=default_args,
    description='Placeholder dag',
    schedule_interval=timedelta(hours=1)) as dag:

    sleep_task = PythonOperator(
        task_id='sleep',
        python_callable=sleep_for,
        op_kwargs={'random_base': float(1) / 10},
        dag=dag,
    )

    sleep_more_task = KubernetesPodOperator(
        name="sleep_more",
        task_id="sleep_more",
        namespace="airflow",
        labels={
            "app": "sleep-more"
        },
        image="ubuntu:20.04",
        image_pull_policy="Always",
        cmds=["sleep"],
        arguments=["5"],
        get_logs=True,
        hostnetwork=False,
        in_cluster=True
    )

    sleep_more_spark_ns_task = KubernetesPodOperator(
        name="sleep_more_spark_ns",
        task_id="sleep_more_spark_ns",
        namespace="spark",
        labels={
            "app": "sleep-more"
        },
        image="ubuntu:20.04",
        image_pull_policy="Always",
        cmds=["sleep"],
        arguments=["5"],
        get_logs=True,
        hostnetwork=False,
        in_cluster=True
    )


    # pylint: disable=pointless-statement
    sleep_task >> [ sleep_more_task, sleep_more_spark_ns_task ]
