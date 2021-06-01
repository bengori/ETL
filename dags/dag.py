from airflow import DAG
from operators.postgres import DataTransferPostgres
from datetime import datetime


DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": datetime(2021, 1, 25),
    "retries": 1,
    "email_on_failure": False,
    "email_on_retry": False,
    "depends_on_past": True,
}

with DAG(
        dag_id="pg-data-flow",
        default_args=DEFAULT_ARGS,
        schedule_interval="@daily",
        max_active_runs=1,
        tags=['data-flow'],
) as dag1:
    files = ['customer', 'supplier', 'nation', 'region', 'orders', 'lineitem', 'part', 'partsupp']
    for file in files:
        tbl = DataTransferPostgres(
            config={'table': f'public.{file}'},
            query=f'select * from {file}',
            task_id=f'{file}',
            source_pg_conn_str="host='db2' port=5432 dbname='tpch' user='admin' password='postgres'",
            pg_conn_str="host='db' port=5432 dbname='my_database2' user='postgres' password='postgres'",
        )
