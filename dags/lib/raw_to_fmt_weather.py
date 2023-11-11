import json
import os
from datetime import date

import pandas as pd

HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/datalake/"
def convert_raw_to_formatted_weather(**kwargs):
   file_name='current_weather.json'
   forecast_file = 'forecast.json'
   current_day = date.today().strftime("%Y%m%d")
   RATING_PATH = DATALAKE_ROOT_FOLDER + "raw/weather/weatherapi/" + current_day + "/" + file_name
   FORECAST_PATH = DATALAKE_ROOT_FOLDER + "raw/weather/weatherapi/" + current_day + "/" + forecast_file
   FORMATTED_RATING_FOLDER = DATALAKE_ROOT_FOLDER + "formatted/weather/weatherapi/" + current_day + "/"
   if not os.path.exists(FORMATTED_RATING_FOLDER):
       os.makedirs(FORMATTED_RATING_FOLDER)

   file = open(RATING_PATH, 'r')
   data = json.load(file)
   # Convert latitude and longitude to strings and then bytes
   data["location"]["lat"] = str(data["location"]["lat"])
   data["location"]["lon"] = str(data["location"]["lon"])
   data["location"]["lat"] = data["location"]["lat"].encode('utf-8')
   data["location"]["lon"] = data["location"]["lon"].encode('utf-8')

   df = pd.json_normalize(data['current'])
   print(df)
   parquet_file_name = file_name.replace(".json", ".snappy.parquet")
   final_df = pd.DataFrame(data=df._data)
   final_df.to_parquet(FORMATTED_RATING_FOLDER + parquet_file_name)

   file_n = open(FORECAST_PATH, 'r')
   data_forecast = json.load(file_n)
   # Convert latitude and longitude to strings and then bytes
   data_forecast["location"]["lat"] = str(data_forecast["location"]["lat"])
   data_forecast["location"]["lon"] = str(data_forecast["location"]["lon"])
   data_forecast["location"]["lat"] = data_forecast["location"]["lat"].encode('utf-8')
   data_forecast["location"]["lon"] = data_forecast["location"]["lon"].encode('utf-8')
   forecast_data = data_forecast['forecast']['forecastday']
   df = pd.DataFrame(forecast_data)
   print(df)
   parquet_file_forecast = forecast_file.replace(".json", ".snappy.parquet")
   final_df_forecast = pd.DataFrame(data=df._data)
   final_df_forecast.to_parquet(FORMATTED_RATING_FOLDER + parquet_file_forecast)

