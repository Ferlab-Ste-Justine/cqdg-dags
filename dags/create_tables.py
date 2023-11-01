from datetime import datetime
from typing import List

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import ParamValidationError
from airflow.models import Param, TaskInstance, DagRun

from lib.config import default_config_file, default_params, K8sContext, variant_task_jar
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
        params=params,
) as dag:
    @task(task_id='get_dataset_ids')
    def get_dataset_ids(**kwargs) -> List[str]:
        ti: TaskInstance = kwargs['ti']
        dag_run: DagRun = ti.dag_run

        if 'dataset_ids' not in dag_run.conf or dag_run.conf['dataset_ids'] == []:
            raise ParamValidationError('Dag param "dataset_ids" is required')

        dataset_ids_args = []
        dataset_ids = dag_run.conf['dataset_ids']
        [dataset_ids_args.extend(['--dataset_id', d]) for d in dataset_ids]
        return dataset_ids_args


    class CreateTableAndView(SparkOperator):
        template_fields = SparkOperator.template_fields + ('arguments', 'dataset_ids',)

        def __init__(self,
                     dataset_ids,
                     **kwargs):
            super().__init__(**kwargs)
            self.dataset_ids = dataset_ids

        def execute(self, **kwargs):
            # Append dataset_ids to arguments at runtime, after dataset_ids has been templated. Otherwise, dataset_ids
            # is interpreted as XComArg and can't be appended to arguments.
            self.arguments = self.arguments + self.dataset_ids
            super().execute(**kwargs)


    CreateTableAndView(task_id='create_table_and_view',
                       name='create-table-and-view',
                       k8s_context=K8sContext.DEFAULT,
                       spark_jar=variant_task_jar,
                       spark_class='bio.ferlab.datalake.spark3.hive.CreateTableAndView',
                       spark_config='etl-task-small',
                       arguments=['--config', default_config_file,
                                  '--steps', 'default',
                                  '--app-name', 'create_table_and_view',
                                  ],
                       dataset_ids=get_dataset_ids())
