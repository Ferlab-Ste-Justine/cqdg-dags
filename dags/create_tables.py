from datetime import datetime
from typing import List

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import ParamValidationError
from airflow.models import Param, TaskInstance, DagRun

from lib.config import K8sContext, variant_task_jar, default_config_file, default_params
from lib.operators.spark import SparkOperator

# Update default params
params = default_params.copy()
params.update({
    'dataset_ids': Param(
        default=[],
        type='array',
        title='Dataset ids',
        description='The dataset ids of the tables you want to create'
    )
})

with DAG(
        dag_id='create_tables',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        params=params
) as dag:
    @task(task_id='get_args')
    def get_args(**kwargs) -> List[str]:
        ti: TaskInstance = kwargs['ti']
        dag_run: DagRun = ti.dag_run

        if 'dataset_ids' not in dag_run.conf or dag_run.conf['dataset_ids'] == []:
            raise ParamValidationError('Dag param "dataset_ids" is required')

        args: List[str] = [
            '--config', default_config_file,
            '--steps', 'default',
            '--app-name', 'create_table_and_view'
        ]
        dataset_ids = dag_run.conf['dataset_ids']
        [args.extend(['--dataset_id', d]) for d in dataset_ids]
        return args


    SparkOperator(
        task_id='create_table_and_view',
        name='create-table-and-view',
        k8s_context=K8sContext.DEFAULT,
        spark_jar=variant_task_jar,
        spark_class='bio.ferlab.datalake.spark3.hive.CreateTableAndView',
        spark_config='etl-index-task',
        arguments=get_args()
    )
