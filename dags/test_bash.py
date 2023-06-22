from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator

#2
with DAG(
        dag_id='test_bash',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
) as dag:


    test_bash = BashOperator(
        task_id='test_bash',
        bash_command="util/es_mapping_import.sh"
    )
