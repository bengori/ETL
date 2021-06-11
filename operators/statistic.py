import psycopg2
from operators.utils import DataFlowBaseOperator
import logging


class DataETLStatisticOperator(DataFlowBaseOperator):
    def __init__(self, config, pg_meta_conn_str, date_check=False, *args, **kwargs):
        super().__init__(config=config, pg_meta_conn_str=pg_meta_conn_str, *args, **kwargs)
        self.config = config
        self.pg_meta_conn_str = pg_meta_conn_str
        self.date_check = date_check

    def execute(self, context):

        schema_name = "{table}".format(**self.config).split(".")
        self.config.update(
            target_schema=schema_name[0],
            target_table=schema_name[1],
            table=schema_name[1],
            job_id=context["task_instance"].job_id,
            load_date=context["task_instance"].execution_date,
        )

        if self.date_check and context["execution_date"] in self.get_load_dates(
                self.config
        ):
            logging.info("Data already load")
            return

        with psycopg2.connect(self.pg_meta_conn_str) as conn, conn.cursor() as cursor:
            cursor.execute(
                """
            select column_name
            from information_schema.columns
            where table_schema = '{target_schema}'
            and table_name = '{table}'
            and column_name not in ('launch_id', 'effective_dttm');
            """.format(
                    **self.config
                )
            )
            results = cursor.fetchall()
            for result in results:
                column = result[0]
                self.config.update(
                    column=column
                )

                cursor.execute(
                    """
                    SELECT count(*)
                    FROM {table};
                """.format(
                        **self.config
                    )
                )
                cnt_all = int(cursor.fetchone()[0])
                self.config.update(cnt_all=cnt_all)

                cursor.execute(
                    """
                    SELECT count(column)
                    FROM {table};
                    """.format(
                        **self.config
                    )
                )
                cnt_not_null = int(cursor.fetchone()[0])
                cnt_nulls = cnt_all - cnt_not_null
                self.config.update(
                    cnt_nulls=cnt_nulls
                )

                self.config.update(
                    launch_id=context["task_instance"].job_id,
                )

                self.write_etl_statistic(self.config)
