import json
import os
from datetime import date

import pandas as pd
from pyspark.sql import SparkSession
HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/datalake/"
def convert_raw_to_formatted_imdb(**kwargs):
   file_name = 'toprated_movies.json'
   file_name2 = 'upcoming_movies.json'
   file_name3 = 'trending_movies.json'
   current_day = date.today().strftime("%Y%m%d")
   RATING_PATH = DATALAKE_ROOT_FOLDER + "raw/movies/imdb/" + current_day + "/" + file_name
   RATING_PATH2 = DATALAKE_ROOT_FOLDER + "raw/movies/imdb/" + current_day + "/" + file_name2
   RATING_PATH3 = DATALAKE_ROOT_FOLDER + "raw/movies/imdb/" + current_day + "/" + file_name3
   FORMATTED_RATING_FOLDER = DATALAKE_ROOT_FOLDER + "formatted/movies/imdb/" + current_day + "/"
   if not os.path.exists(FORMATTED_RATING_FOLDER):
       os.makedirs(FORMATTED_RATING_FOLDER)

   file = open(RATING_PATH, 'r')
   data1 = json.load(file)
   movies = data1['results']
   df = pd.DataFrame(data=movies)
   print(df)
   parquet_file_name = file_name.replace(".json", ".snappy.parquet")
   final_df = pd.DataFrame(data=df._data)
   final_df.to_parquet(FORMATTED_RATING_FOLDER + parquet_file_name)

   file2 = open(RATING_PATH2, 'r')
   data2 = json.load(file2)
   movies2 = data2['results']
   df2 = pd.DataFrame(data=movies2)
   print(df2)
   parquet_file_name2 = file_name2.replace(".json", ".snappy.parquet")
   final_df2 = pd.DataFrame(data=df2._data)
   final_df2.to_parquet(FORMATTED_RATING_FOLDER + parquet_file_name2)

   file3 = open(RATING_PATH3, 'r')
   data3 = json.load(file3)
   movies3 = data3['results']
   df3 = pd.DataFrame(data=movies3)
   print(df3)
   parquet_file_name3 = file_name3.replace(".json", ".snappy.parquet")
   final_df3 = pd.DataFrame(data=df3._data)
   final_df3.to_parquet(FORMATTED_RATING_FOLDER + parquet_file_name3)


