from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.models import DagRun
from airflow.models import Param

from cqdg.lib.config import default_config_file, study_code, etl_variant_config
from cqdg.lib.operators.spark import SparkOperator
from cqdg.lib.slack import Slack


class NormalizeVariants(SparkOperator):
    template_fields = [*SparkOperator.template_fields, 'arguments', 'dataset_batch']

    def __init__(self,
                 dataset_batch,
                 **kwargs):
        super().__init__(**kwargs)
        self.dataset_batch = dataset_batch

    def execute(self, **kwargs):
        # Append dataset and batch to arguments at runtime.
        self.arguments.append('--dataset')
        self.arguments.append(self.dataset_batch[0])
        self.arguments.append('--batch')
        self.arguments.append(self.dataset_batch[1])
        super().execute(**kwargs)

@task
def extract_params(ti=None) -> list[(str, list[str])]:
    """Extract input arguments at runtime.
    Returns: List of datasets with their batches
    """
    dag_run: DagRun = ti.dag_run
    items = dag_run.conf["dateset_batches"]
    r_list = []

    for item in items:
        bs = item['batches']
        d = item['dataset']
        for b in bs:
            r_list.append((d, b))
    return r_list

def normalized_etl(run_time_params, name):
    return (etl_variant_config
            .with_spark_class('bio.ferlab.etl.normalized.RunNormalizedGenomic')
            .prepend_args(name)
            .args(
        '--config', default_config_file,
        '--steps', 'default',
        '--app-name', 'variant_task_consequences',
        '--owner', '{{ params.owner }}',
        '--study-code', study_code)
            .add_package('io.projectglow:glow-spark3_2.12:2.0.0')
            .add_spark_conf({'spark.jars.excludes': 'org.apache.hadoop:hadoop-client,'
                                                    'io.netty:netty-all,'
                                                    'io.netty:netty-handler,'
                                                    'io.netty:netty-transport-native-epoll',
                             'spark.hadoop.io.compression.codecs': 'io.projectglow.sql.util.BGZFCodec',
                             })
            .partial(
        class_to_instantiate=NormalizeVariants,
        task_id=f'normalize-{name}',
        name=f'normalize-{name}')
            .expand(dataset_batch=run_time_params))


with DAG(
        dag_id='etl-normalize-variants',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        # concurrency set to 1, only one task can run at a time to avoid conflicts in Delta table
        concurrency=1,
        params={
            'study_code': Param('CAG', type='string'),
            'owner': Param('jmichaud', type='string'),
            'project': Param('cqdg', type='string'),
            'dateset_batches': Param(
                [
                    {'dataset': 'dataset_dataset1', 'batches': ['annotated_vcf1','annotated_vcf2']},
                    {'dataset': 'dataset_dataset2', 'batches': ['annotated_vcf']}
                ],
                schema =  {
                    "type": "array",
                    "minItems": 1,
                    "items": {
                        "type": "object",
                        "default": {'dataset': 'dataset_default', 'batches': ['annotated_vcf']},
                        "properties": {
                            "dataset": {"type": "string"},
                            "batches": {"type": "array", "items": {"type": "string"}},
                        },
                        "required": ["dataset", "batches"]
                    },
                }
            ),
        },
        on_failure_callback=Slack.notify_task_failure
) as dag:
    params = extract_params()

    normalized_etl(run_time_params = params, name='snv') >> normalized_etl(run_time_params = params, name='consequences')