from airflow import DAG
from airflow.models.param import Param
from datetime import datetime
from lib.config import etl_index_config, env, es_url, es_port, study_codes, release_id, project, etl_index_config
def index_operator(name:str):
    return etl_index_config.with_spark_class('bio.ferlab.fhir.etl.IndexTask') \
                .args(release_id, study_codes, f'{name}_centric', env, project, es_url, es_port) \
                .operator(
                    task_id=f'{name}_centric',
                    name=f'etl-index-{name}-centric'
                )
with DAG(
        dag_id='etl-index',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        params={
            'release_id': Param('7', type='string'),
            'study_codes': Param('CAG', type='string'),
            'project': Param('cqdg', type='string')
        },
) as dag: 
    index_operator('study') >> index_operator('participant') >> index_operator('file') >>  index_operator('biospecimen') 

