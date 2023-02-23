import requests
from bs4 import BeautifulSoup
import pandas
import openpyxl
import datetime

url = "https://www.timeanddate.com/weather/china/shanghai/ext"

r=requests.get(url)
c = r.content

soup = BeautifulSoup(c,"html.parser") #Return BS Object
source = str(soup.find_all("table",{"id":"wt-ext"})) #Return string
mySource = BeautifulSoup(source,"html.parser") #Return BS Object

# Get the content from the table body:
tableBody = str(mySource.find_all("tbody")) #Return string
tbContent = BeautifulSoup(tableBody,"html.parser") #Return BS Object

# Get all the dates and store in an array:
dates = tbContent.find_all("th") #Return BS Result Sets
dates_array = []

year = datetime.date.today().year
for date in dates:
    month = int(date.get_text().split("月")[0][3])
    day = int(date.get_text().split("月")[1].split("日")[0])
    dates_array.append(datetime.date(year,month,day))
    # dates_array.append(date.get_text())

# Get table rows for each day:
trData = tbContent.find_all("tr") #Return BS Result Sets

tempHigh = []
tempLow = []
weather = []
feelsLike = []
windSpeed = []
humidity = []
precip_chance = []
precip_amount = []

for row in trData:
    # Parse table row and search for td from it:
    tdContent = BeautifulSoup(str(row),"html.parser")
    tdData = tdContent.find_all("td")

    # Get all data
    tempHigh.append(int(tdData[1].get_text().split("/")[0]))
    tempLow.append(int(tdData[1].get_text().split("/")[1].split("\u00A0")[0]))
    weather.append(tdData[2].get_text())
    feelsLike.append(int(tdData[3].get_text().split("\u00A0")[0]))
    windSpeed.append(int(tdData[4].get_text().split(" ")[0]))
    humidity.append(int(tdData[6].get_text().split("%")[0]))
    precip_chance.append(int(tdData[7].get_text().split("%")[0]))
    precip_amount.append(tdData[8].get_text().split(" ")[0])


# Store all data in the dataframe:
data = {"Date": dates_array,
        "High Temp": tempHigh,
        "Low Temp": tempLow,
        "Weather": weather,
        "Feels Like": feelsLike,
        "Wind Speed": windSpeed,
        "Humidity": humidity,
        "Precip Chance": precip_chance,
        "Precip Amount": precip_amount}

df = pandas.DataFrame(data)
print(df)

#df_initial = pandas.DataFrame()
#with pandas.ExcelWriter('WeatherResults.xlsx') as writer:
#    df_initial.to_excel(writer, "Shanghai")

# Write data into excel file:
with pandas.ExcelWriter(
        'WeatherResults.xlsx',
        mode="a",
        engine="openpyxl",
        if_sheet_exists="overlay",
        date_format="YYYY-MM-DD"
) as writer:
    df.to_excel(writer, "Shanghai") #,startrow=writer.sheets["Shanghai"].max_row)
