import datetime
import pandas
import json


df = pandas.read_json('temperature.json')
df.drop(df[df['time'] == 30].index, inplace=True)

print(df)