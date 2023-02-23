import requests
from bs4 import BeautifulSoup
import pandas
import json
import datetime
import openpyxl

base_url = "https://www.timeanddate.com/weather/usa/buffalo/historic?month="
startYear = 2011
endYear = 2011
Variable = "Temp"
threshold = -3     #temperature in F

Temperature = []
for year in range(startYear,endYear+1):
    for month in range(1,13):
        url = base_url + str(month) + "&year=" + str(year)
        
        r=requests.get(url)
        c=r.content

        soup = BeautifulSoup(c,"html.parser")
        #print(soup.prettify())

        source = str(soup.find_all("script",{"type":"text/javascript"})[1])
        startIndex = source.find('{')
        endIndex = source.rfind("}")
        source = source[startIndex:endIndex+1]
        source

        data = json.loads(source)
        for item in data[Variable.lower()]:
            Temperature.append(item)

        Temperature

df = pandas.DataFrame(Temperature)

Date = []
Time = []
for d in df["date"]:
    time = datetime.datetime.fromtimestamp(d/1000).isoformat()
    d = time.split("T")[0]
    t = time.split("T")[1]
    Date.append(d)
    Time.append(t)
    
df["date"] = Date
df.insert(loc=1,column="time",value=Time)
#df
target_df = df[df["temp"]<threshold]
target_df = target_df.rename(columns={"temp":"Temperature(F)"})


writer = pandas.ExcelWriter('WeatherResults.xlsx', engine='xlsxwriter')
df.to_excel(writer,'All_hour',index=False)
target_df.to_excel(writer,'Condensation_hour',index=False)
writer.save()
