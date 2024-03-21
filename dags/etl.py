from airflow import DAG
from airflow.models.param import Param
from airflow.utils.task_group import TaskGroup
from datetime import datetime
from lib.config import env, study_codes, release_id, es_port, es_url, etl_publish_config, project
from es_templates_update import es_templates_update
from etl_import import etl_import
from arranger import arranger_task
from etl_fhavro_export import fhavro_export
from etl_index import index_operator
from etl_prepare_index import prepare_index

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

    with TaskGroup(group_id='publish') as publish:
       def publish_operator(name:str):
            return etl_publish_config \
                        .args(es_url, es_port, env, project, release_id, study_codes, f'{name}_centric') \
                        .operator(
                            task_id=f'{name}_centric',
                            name=f'etl-publish-{name}-centric'
                        )            
              
       publish_operator('study') >> publish_operator('participant') >> publish_operator('file') >>  publish_operator('biospecimen') 
    
    fhavro_export() >> etl_import() >> prepare_index() >> es_templates_update() >> index >> publish >> arranger_task()
