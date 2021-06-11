import psycopg2

conn_string = "host='localhost' port=5433 dbname='tpch_source' user='root' password='postgres'"
with psycopg2.connect(conn_string) as conn, conn.cursor() as cursor:
    cursor.execute(
        """
        SELECT count(*)
        FROM customer;
    """
    )
    cnt_all = int(cursor.fetchone()[0])
    print(cnt_all)


    cursor.execute(
        """
        select column_name
        from information_schema.columns
        where information_schema.columns.table_name='customer';
        """
    )
    results = cursor.fetchall()
    for result in results:
        column = result[0]
        cursor.execute(
            f"""
            SELECT count({column})
            FROM customer;
        """
        )
        cnt_not_null = int(cursor.fetchone()[0])
        print(f'для {column} cnt_not_null: {cnt_not_null}')
        cnt_nulls = cnt_all - cnt_not_null
        print(f'для {column} cnt_nulls: {cnt_nulls}')
