import psycopg2


connect = psycopg2.connect(
    database="Kunyu_Test",
    user="postgres",
    password="pg123",
    host="127.0.0.1",
    port="5432")

cursor = connect.cursor()

table_name = """ "Teams" """
col1 = "Player"
col2 = "Team"
sql = "INSERT INTO{}(\"Player\", \"Team\", \"Number\") VALUES(%s, %s, %s)".format(table_name)
# print(sql)

names = ["Anthony", "Bulter"]
teams = ["Denver", "Heat"]
numbers = [35, 45]

data = zip(names, teams, numbers)
data_list = [list(d) for d in data]

print(data_list)

# cursor.execute(sql, values)
try:
    cursor.executemany(sql, data_list)
except Exception as e:
    print(e)

connect.commit()
cursor.close()
connect.close()
