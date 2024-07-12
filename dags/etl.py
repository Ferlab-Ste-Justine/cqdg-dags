from datetime import datetime

from airflow import DAG
from airflow.models.param import Param
from airflow.utils.task_group import TaskGroup

from es_templates_update import es_templates_update
from etl_fhavro_export import fhavro_export
from etl_import import etl_import
from etl_index import index_operator
from etl_prepare_index import prepare_index
from etl_publish import publish_task

with DAG(
        dag_id='etl',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        params={
            'release_id': Param('7', type='string'),
            'study_codes': Param('CAG', type='string'),
            'project': Param('cqdg', type='string'),
            'es_port': Param('9200', type='string')
        },
) as dag:

    with TaskGroup(group_id='index') as index:
        index_operator('study') >> index_operator('participant') >> index_operator('file') >> index_operator('biospecimen')
    
    fhavro_export() >> etl_import() >> prepare_index() >> es_templates_update() >> index >>  publish_task('study_centric,participant_centric,file_centric,biospecimen_centric')
