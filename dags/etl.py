from datetime import datetime

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

from lib import config
from lib.config import env, Env, K8sContext

if env in [Env.QA, Env.DEV]:

    with DAG(
            dag_id='etl',
            start_date=datetime(2022, 1, 1),
            schedule_interval=None,
            params={
                # 'release_id': Param('5', type='string'),
                # 'study_ids': Param('STU0000001', type='string'),
                # 'jobType': Param('participant_centric', type='string'),
                # 'env': Param('', enum=['dev', 'qa', 'prd']),
                # 'project': Param('cqdg', type='string'),
            },
    ) as dag:

        # fhavro_export = FhavroOperator(
        #     task_id='fhavro_export',
        #     name='etl-fhavro-export',
        #     k8s_context=K8sContext.DEFAULT,
        #     arguments=[
        #         '5', 'STU0000001', f'{env}',
        #     ],
        # )

        # index_task = SparkOperator(
        #     task_id='index-task',
        #     name='etl-index-task',
        #     k8s_context=K8sContext.ETL,
        #     spark_jar=config.spark_jar_index_task,
        #     spark_class='bio.ferlab.fhir.etl.IndexTask',
        #     spark_config='enriched-etl',
        #     arguments=['./config/dev-cqdg.conf', 'default', '5', 'STU0000001'],
        # )

        test_pod_operator_default = KubernetesPodOperator(
            task_id='test_pod_operator_default',
            name='test-pod-operator-default',
            is_delete_operator_pod=True,
            in_cluster=config.k8s_in_cluster(K8sContext.DEFAULT),
            # config_file=config.k8s_config_file(K8sContext.DEFAULT),
            config_file='~/.kube/config',
            cluster_context='kubernetes-admin-cluster.tunnel.qa@cluster.tunnel.qa',
            namespace='cqdg-qa',
            image='alpine',
            cmds=['echo', 'hello'],
            arguments=[],
        )

        # fhavro_export >> import_tesk
