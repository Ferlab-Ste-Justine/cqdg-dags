# from airflow import DAG
# from airflow.models.param import Param

# from datetime import datetime

# # from lib import config
# # from lib.operators.fhavro import FhavroOperator
# # from lib.config import env, Env, K8sContext
# # from lib.operators.spark import SparkOperator
# # if env in [Env.QA, Env.DEV]:

# with DAG(
#         dag_id='etl',
#         start_date=datetime(2022, 1, 1),
#         schedule_interval=None,
#         params={
#             'release_id': Param('5', type='string'),
#             'study_ids': Param('STU0000001', type='string'),
#             'jobType': Param('participant_centric', type='string'),
#             'env': Param('', enum=['dev', 'qa', 'prd']),
#             'project': Param('cqdg', type='string'),
#         },
# ) as dag:

#     # fhavro_export_task = FhavroOperator(
#     #     task_id='fhavro-import-task',
#     #     name='etl-fhavro-import-task',
#     #     k8s_context=K8sContext.ETL,
#     #     spark_jar=config.spark_index_jar,
#     #     spark_class='bio.ferlab.fhir.etl.FhavroExport',
#     #     spark_config='enriched-etl',
#     #     arguments=['7', 'ST0000017', 'dev'],
#     # )
#     #
#     # import_task = SparkOperator(
#     #     task_id='import-task',
#     #     name='etl-import-task',
#     #     k8s_context=K8sContext.ETL,
#     #     spark_jar=config.spark_index_jar,
#     #     spark_class='bio.ferlab.fhir.etl.ImportTask',
#     #     spark_config='enriched-etl',
#     #     arguments=['./config/dev-cqdg.conf', 'default', '5', 'STU0000001'],
#     # )
#     #
#     # prepare_index_task = SparkOperator(
#     #     task_id='prepare-index-task',
#     #     name='etl-prepare-index-task',
#     #     k8s_context=K8sContext.ETL,
#     #     spark_jar=config.spark_index_jar,
#     #     spark_class='bio.ferlab.fhir.etl.PrepareIndex',
#     #     spark_config='enriched-etl',
#     #     arguments=['./config/dev-cqdg.conf', 'default', 'participant_centric', '5', 'STU0000001'],
#     # )

#     index_task = SparkOperator(
#         task_id='index-task',
#         name='etl-index-task',
#         # k8s_context=K8sContext.ETL,
#         k8s_context='kubernetes_context_etl',
#         spark_jar='https://github.com/Ferlab-Ste-Justine/etl-cqdg-portal/releases/download/v1.0.1/index-task.jar',
#         spark_class='bio.ferlab.fhir.etl.IndexTask',
#         spark_config='enriched-etl',
#         arguments=['./config/dev-cqdg.conf', 'default', '5', 'STU0000001'],
#     )
