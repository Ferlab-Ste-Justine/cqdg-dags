from datetime import datetime

from airflow import DAG
from airflow.models.param import Param

from dags.lib.operators.fhir import FhirOperator
from dags.lib.operators.wait import WaitOperator
from lib.config import K8sContext
from lib.operators.fhir_import import FhirCsvOperator

with DAG(
    dag_id='etl_import_fhir',
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    params={
        'prefix': Param('clinical-data/e2adb961-4f58-4e13-a24f-6725df802e2c', type='string'),
        'bucket': Param('ST0000017', type='string'),
        'version': Param('cqdg', type='string'),
        'release': Param('9200', type='string'),
        'study': Param('9200', type='string'),
        'run_names': Param('1615,1616,1617,1644,1645,1646,1647,1650,1651,1656,1658,1659,1660,1661,1667,1668,1669,1680,'
                           '1681,1682,1690,1691,1692,1704,1707,1735,1736,1737,1738,1748,1750,1854,1855,1856,1857,1860,'
                           '1861,1862,1865,1866,1867,1872,1873,1876,1877,1881,1882,1884,1885,1886,1887,1894,1895,1898,'
                           '1899,1902,1903,1906,1907,1916,1917,1918,1919,1920,1921,1922,1924,1925,1926,1927,1928,1929,'
                           '1930,1934,1935,1951,1952,A00516_0270,A00516_0271', type='string')
    },
) as dag:

    def prefix() -> str:
        return '{{ params.prefix }}'

    def bucket() -> str:
        return '{{ bucket.prefix }}'

    def version() -> str:
        return '{{ version.prefix }}'

    def release() -> str:
        return '{{ release.prefix }}'

    def study() -> str:
        return '{{ study.prefix }}'

    def run_names() -> str:
        return '{{ run_names.prefix }}'

    ig_publish = FhirOperator(
        task_id='ig_publish',
        name='etl-import-fhir-ig-publish',
        k8s_context=K8sContext.DEFAULT,
    )

    wait_30s = WaitOperator(
        task_id='wait_30s',
        time='30s',
    )

    csv_import = FhirCsvOperator(
        task_id='fhir_import',
        name='etl-fhir_import',
        k8s_context=K8sContext.DEFAULT,
        arguments=["-cp", "cqdg-fhir-import.jar", "bio/ferlab/cqdg/etl/FhirImport",
                   prefix(), bucket(), version(), release(), study(), "true", run_names()],
    )

    ig_publish >> wait_30s >> csv_import
