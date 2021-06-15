import psycopg2

conn_string = "host='localhost' port=54320 dbname='target' user='root' password='postgres'"
with psycopg2.connect(conn_string) as conn, conn.cursor() as cursor:
    cursor.execute('select count(*) from customer')
    print(cursor.fetchall())
