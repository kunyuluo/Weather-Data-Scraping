import psycopg2 as pc


connection = pc.connect(
    host="localhost",
    port="5432",
    user="postgres",
    password="pg123")

connection.close()