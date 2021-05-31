import psycopg2

# извлечение данных из источника бд - 'my_database'
conn_string = "host='localhost' port=54320 dbname='my_database' user='root' password='postgres'"
files = ['customer', 'supplier', 'nation', 'region', 'orders', 'lineitem', 'part', 'partsupp']

with psycopg2.connect(conn_string) as conn, conn.cursor() as cursor:
    for file in files:
        q = f"COPY {file} TO STDOUT WITH DELIMITER ',' CSV HEADER;"
        with open(f'{file}.csv', 'w') as f:
            cursor.copy_expert(q, f)

# загрузка данных в хранилище бд - 'my_database2'
conn_string2 = "host='localhost' port=5433 dbname='my_database2' user='root' password='postgres'"
with psycopg2.connect(conn_string2) as conn, conn.cursor() as cursor:
    for file in files:
        q = f"COPY {file} from STDIN WITH DELIMITER ',' CSV HEADER;"
        with open(f'{file}.csv', 'r') as f:
            cursor.copy_expert(q, f)
        cursor.execute(f'select count(*) from {file}')
        print(cursor.fetchall())  # сколько записей в хранилище в конкретной таблице
