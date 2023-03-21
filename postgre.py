import psycopg2


def insert_data_into_database(names, teams, numbers, table_name="Teams"):

    # First, build a connection
    connect = psycopg2.connect(
        database="Kunyu_Test",
        user="postgres",
        password="pg123",
        host="10.252.32.149",
        port="5432")

    # Set up a cursor
    cursor = connect.cursor()

    names_f, teams_f, numbers_f = [], [], []
    # data_length = min(len(names), len(teams), len(numbers))
    data_length = len(names)
    for i in range(data_length):
        names_f.extend([names[i]])
        teams_f.extend([teams[i]])
        numbers_f.extend([numbers[i]])

    # Command line to write data into table
    table_name = '"' + table_name + '"'
    sql = "INSERT INTO{}(\"Player\", \"Team\", \"Number\") VALUES(%s, %s, %s)".format(table_name)

    # Group datasets into a list
    data = zip(names_f, teams_f, numbers_f)
    data_list = [list(d) for d in data]

    # Execute
    try:
        cursor.executemany(sql, data_list)
    except Exception as e:
        print(e)

    connect.commit()
    cursor.close()
    connect.close()


my_names = ["James", "Kobe", "Durant"]
my_teams = ["Lakers"]
my_numbers = []

insert_data_into_database(my_names, my_teams, my_numbers)
