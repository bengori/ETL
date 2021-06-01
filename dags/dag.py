from airflow import DAG
from operators.postgres import DataTransferPostgres
from datetime import datetime


DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": datetime(2021, 5, 31),
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
    # ['customer', 'supplier', 'nation', 'region', 'orders', 'lineitem', 'part', 'partsupp']

    t1 = DataTransferPostgres(
        config={'table': 'public.customers'},
        query='select * from customers',
        task_id='customers',
        source_pg_conn_str="host='db2' port=5432 dbname='my_database_source' user='root' password='postgres'",
        pg_conn_str="host='db' port=5432 dbname='my_database_target' user='root' password='postgres'",
    )
    t2 = DataTransferPostgres(
        config={'table': 'public.supplier'},
        query='select * from supplier',
        task_id='supplier',
        source_pg_conn_str="host='db2' port=5432 dbname='my_database_source' user='root' password='postgres'",
        pg_conn_str="host='db' port=5432 dbname='my_database_target' user='root' password='postgres'",
    )

    t1 >> t2
