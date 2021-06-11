import psycopg2

conn_string = "host='localhost' port=5433 dbname='tpch_source' user='root' password='postgres'"
with psycopg2.connect(conn_string) as conn, conn.cursor() as cursor:
    cursor.execute('select count(*) from customer')
    print(cursor.execute(cursor.fetchall()))
