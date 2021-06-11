from airflow import DAG
from operators.postgres import DataTransferPostgres
from datetime import datetime

DEFAULT_ARGS = {
    "owner": "airflow2",
    "start_date": datetime(2021, 5, 31),
    "retries": 1,
    "email_on_failure": False,
    "email_on_retry": False,
    "depends_on_past": True,
}

with DAG(
        dag_id = "pg-data-flow1",
        default_args = DEFAULT_ARGS,
        schedule_interval = "@daily",
        max_active_runs = 1,
        tags = ['data-flow1'],
) as dag1:
    t1 = DataTransferPostgres(
        config = {'table': 'public.customer'},
        query = 'select * from customer',
        task_id = 'customer',
        source_pg_conn_str = "host= 'db' port = 5432 dbname = 'tpch_source' user = 'root' password = 'postgres'",
        pg_conn_str = "host = 'db2' port = 5432 dbname = 'target' user = 'root' password = 'postgres'",
        pg_meta_conn_str = "host = 'db2' port = 5432 dbname= 'target' user = 'root' password = 'postgres'", # modify
    )

    """
    t2 = DataTransferPostgres(
        config = {'table': 'public.supplier'},
        query = 'select * from supplier',
        task_id = 'supplier',
        source_pg_conn_str = "host='db' port=5432 dbname='tpch_source' user='root' password='postgres'",
        pg_conn_str = "host='db2' port=5432 dbname='target' user='root' password='postgres'",
        pg_meta_conn_str = "host='db2' port=5432 dbname='target' user='root' password='postgres'", # modify
    )

    t3 = DataTransferPostgres(
        config={'table': 'public.lineitem'},
        query='select * from lineitem',
        task_id='lineitem',
        source_pg_conn_str="host='db' port=5432 dbname='tpch_source' user='root' password='postgres'",
        pg_conn_str="host='db2' port=5432 dbname='target' user='root' password='postgres'",
        pg_meta_conn_str="host='db2' port=5432 dbname='target' user='root' password='postgres'", # modify
    )

    t4 = DataTransferPostgres(
        config = {'table': 'public.nation'},
        query = 'select * from nation',
        task_id = 'nation',
        source_pg_conn_str = "host='db' port=5432 dbname='tpch_source' user='root' password='postgres'",
        pg_conn_str = "host='db2' port=5432 dbname='target' user='root' password='postgres'",
        pg_meta_conn_str = "host='db2' port=5432 dbname='target' user='root' password='postgres'", # modify
    )

    t5 = DataTransferPostgres(
        config = {'table': 'public.orders'},
        query = 'select * from orders',
        task_id = 'orders',
        source_pg_conn_str = "host='db' port=5432 dbname='tpch_source' user='root' password='postgres'",
        pg_conn_str = "host='db2' port=5432 dbname='target' user='root' password='postgres'",
        pg_meta_conn_str = "host='db2' port=5432 dbname='target' user='root' password='postgres'", # modifyfy
    )

    t6 = DataTransferPostgres(
        config = {'table': 'public.part'},
        query = 'select * from part',
        task_id = 'part',
        source_pg_conn_str = "host='db' port=5432 dbname='tpch_source' user='root' password='postgres'",
        pg_conn_str = "host='db2' port=5432 dbname='target' user='root' password='postgres'",
        pg_meta_conn_str = "host='db2' port=5432 dbname='target' user='root' password='postgres'", # modify
    )

    t7 = DataTransferPostgres(
        config = {'table': 'public.partsupp'},
        query = 'select * from partsupp',
        task_id = 'partsupp',
        source_pg_conn_str = "host='db' port=5432 dbname='tpch_source' user='root' password='postgres'",
        pg_conn_str = "host='db2' port=5432 dbname='target' user='root' password='postgres'",
        pg_meta_conn_str = "host='db2' port=5432 dbname='target' user='root' password='postgres'", # modify
    )

    t8 = DataTransferPostgres(
        config = {'table': 'public.region'},
        query = 'select * from region',
        task_id = 'region',
        source_pg_conn_str = "host='db' port=5432 dbname='tpch_source' user='root' password='postgres'",
        pg_conn_str = "host='db2' port=5432 dbname='target' user='root' password='postgres'",
        pg_meta_conn_str = "host='db2' port=5432 dbname='target' user='root' password='postgres'", # modify
    )
    
    t1 >> t2 >> t3 >> t4 >> t5
    t1 >> t6 >> t7 >> t8
    """