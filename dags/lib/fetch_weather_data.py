import json
import os
from datetime import date

import requests

HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/datalake/"
def fetch_data_from_weather(**kwargs):
   current_day = date.today().strftime("%Y%m%d")
   TARGET_PATH = DATALAKE_ROOT_FOLDER + "raw/weather/weatherapi/" + current_day + "/"
   if not os.path.exists(TARGET_PATH):
       os.makedirs(TARGET_PATH)


   url="http://api.weatherapi.com/v1/current.json?key=ff8383084ea44400a97181602232705&q=Paris"

   # Sending the API request
   r = requests.get(url)
   open(TARGET_PATH + 'current_weather.json', 'wb').write(r.content)

   url_forecast = "https://api.weatherapi.com/v1/forecast.json"
   api_key = "ff8383084ea44400a97181602232705"
   location = "Paris"
   params = {
      "key": api_key,
      "q": location,
      "days": 7,  # Number of days for the forecast (adjust as needed)
   }
   response = requests.get(url_forecast, params=params)
   open(TARGET_PATH + 'forecast.json', 'wb').write(response.content)

