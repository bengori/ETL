from airflow import DAG
from operators.statistic import DataETLStatisticOperator
from datetime import datetime
from airflow.utils.state import State
from airflow.sensors.external_task_sensor import ExternalTaskSensor

DEFAULT_ARGS = {
    "owner": "airflow2",
    "start_date": datetime(2021, 5, 31),
    "retries": 1,
    "email_on_failure": False,
    "email_on_retry": False,
    "depends_on_past": True,
}

with DAG(
        dag_id="pg-wrt-statistic",
        default_args=DEFAULT_ARGS,
        schedule_interval="@daily",
        max_active_runs=1,
        tags=['wrt-statistic'],
) as dag:
    sensor1 = ExternalTaskSensor(
        external_task_id='customer',
        external_dag_id='pg-data-flow1',
        task_id='customer',
        check_existence=True,
        allowed_states=[State.SUCCESS],
        retries=0,
    )

    statistic_customer = DataETLStatisticOperator(
        config={'table': 'public.customer'},
        task_id='statistic_customer',
        pg_conn_str="host='db2' port=5432 dbname='target' user='root' password='postgres'",
        pg_meta_conn_str="host='db2' port=5432 dbname='target' user='root' password='postgres'",
    )

    sensor1 >> statistic_customer
