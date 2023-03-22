import requests
from bs4 import BeautifulSoup
import pandas
import json
import datetime
import psycopg2

base_url = "https://www.timeanddate.com/weather/china/shanghai/historic?month={}&year={}"
startYear = 2023
endYear = 2023
start_month = 1
end_month = 3
Variable = "Temp"

Temperature = []
for year in range(startYear, endYear + 1):
    for month in range(start_month, end_month + 1):
        url = base_url.format(str(month), str(year))

        r = requests.get(url)
        c = r.content

        soup = BeautifulSoup(c, "html.parser")

        source = str(soup.find_all("script", {"type": "text/javascript"})[1])
        startIndex = source.find('{')
        endIndex = source.rfind("}")
        source = source[startIndex:endIndex + 1]

        data = json.loads(source)
        for item in data[Variable.lower()]:
            Temperature.append(item)

df = pandas.DataFrame(Temperature)

years, months, days, hours, minutes, time_stamps = [], [], [], [], [], []

for d in df["date"]:
    time = datetime.datetime.fromtimestamp(d / 1000).isoformat()

    year = time.split("T")[0].split("-")[0]
    month = time.split("T")[0].split("-")[1]
    day = time.split("T")[0].split("-")[2]
    hour = time.split("T")[1].split(":")[0]
    minute = time.split("T")[1].split(":")[1]

    years.append(year)
    months.append(month)
    days.append(day)
    hours.append(hour)
    minutes.append(minute)
    time_stamps.append(d)

df.insert(loc=1, column="Year", value=years)
df.insert(loc=2, column="Month", value=months)
df.insert(loc=3, column="Day", value=days)
df.insert(loc=4, column="Hour", value=hours)
df.insert(loc=5, column="Minute", value=minutes)
df.insert(loc=6, column="Timestamp", value=time_stamps)
df.drop(columns=["date"], inplace=True)

# Delete "half-hour" data point:
df.drop(df[df['Minute'] == "30"].index, inplace=True)

# Check output:
# print(df)


def insert_data_into_database(years, months, days, hours, time_stamps, temperature, table_name="WeatherData"):
    # First, build a connection
    connect = psycopg2.connect(
        database="ClimateData",
        user="postgres",
        password="pg123",
        host="127.0.0.1",
        port="5432")

    # Set up a cursor
    cursor = connect.cursor()

    years_f, months_f, days_f, hours_f, time_stamps_f, temperature_f = [], [], [], [], [], []
    data_length = min(len(years), len(months), len(days), len(hours), len(time_stamps), len(temperature))
    for i in range(data_length):
        years_f.extend([years[i]])
        months_f.extend([months[i]])
        days_f.extend([days[i]])
        hours_f.extend([hours[i]])
        time_stamps_f.extend([time_stamps[i]])
        temperature_f.extend([temperature[i]])

    # Command line to write data into table
    table_name = '"' + table_name + '"'
    sql = "INSERT INTO{}(\"Year\", \"Month\", \"Day\", \"Hour\", \"Timestamp\", \"Temperature\") VALUES(%s, %s, %s, " \
          "%s, %s, %s)".format(table_name)

    # Group datasets into a list
    dataset = zip(years_f, months_f, days_f, hours_f, time_stamps_f, temperature_f)
    data_list = [list(d) for d in dataset]

    # Execute
    try:
        cursor.executemany(sql, data_list)
    except Exception as e:
        print(e)

    connect.commit()
    cursor.close()
    connect.close()


def update_data_in_database(keys, data, table_name="WeatherData"):
    # First, build a connection
    connect = psycopg2.connect(
        database="ClimateData",
        user="postgres",
        password="pg123",
        host="127.0.0.1",
        port="5432")

    # Set up a cursor
    cursor = connect.cursor()

    # Command line to write data into table
    table_name = '"' + table_name + '"'
    for i in range(len(keys)):
        sql = "UPDATE {} SET \"Humidity\"={} WHERE \"Timestamp\"={};".format(table_name, str(data[i]), str(keys[i]))

        # Execute
        try:
            cursor.execute(sql)
        except Exception as e:
            print(e)

    connect.commit()
    cursor.close()
    connect.close()


insert_data_into_database(
    df['Year'].to_list(), df['Month'].to_list(), df['Day'].to_list(),
    df['Hour'].to_list(), df['Timestamp'].to_list(), df['temp'].to_list())

# update_data_in_database(df['Timestamp'].to_list(), df['temp'].to_list())
