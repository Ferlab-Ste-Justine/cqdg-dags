from datetime import datetime
from typing import List

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import ParamValidationError
from airflow.models import Param, TaskInstance, DagRun

from cqdg.lib.config import spark_small_conf, default_config_file, default_params, variant_jar, etl_base_config
from cqdg.lib.operators.spark import SparkOperator

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
        dag_id='create-tables',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        params=params,
) as dag:
    @task(task_id='get_dataset_ids')
    def get_dataset_ids(**kwargs) -> List[str]:
        ti: TaskInstance = kwargs['ti']
        dag_run: DagRun = ti.dag_run

        if 'dataset_ids' not in dag_run.conf or dag_run.conf['dataset_ids'] == []:
            raise ParamValidationError('Dag param "dataset_ids" is required')

        return dag_run.conf['dataset_ids']


    class CreateTableAndView(SparkOperator):
        template_fields = [*SparkOperator.template_fields, 'arguments', 'dataset_id']

        def __init__(self,
                     dataset_id,
                     **kwargs):
            super().__init__(**kwargs)
            self.dataset_id = dataset_id

        def execute(self, **kwargs):
            # Append dataset_ids to arguments at runtime, after dataset_ids has been templated. Otherwise, dataset_ids
            # is interpreted as XComArg and can't be appended to arguments.
            self.arguments.append('--dataset_id')
            self.arguments.append(self.dataset_id)
            super().execute(**kwargs)


    etl_base_config.add_spark_conf(spark_small_conf) \
        .with_spark_class('bio.ferlab.datalake.spark3.hive.CreateTableAndView') \
        .with_spark_jar(variant_jar) \
        .args('--config', default_config_file,
              '--steps', 'default',
              '--app-name', 'create_table_and_view'
              ) \
        .partial(
            class_to_instantiate=CreateTableAndView,
            task_id='create_table_and_view',
            name='create-table-and-view'
        ) \
        .expand(dataset_id=get_dataset_ids())
