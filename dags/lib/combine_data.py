import os
from datetime import date

from pyspark.sql import SQLContext
from pyspark import SparkContext

HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/datalake/"


def combine_data():
   current_day = date.today().strftime("%Y%m%d")
   toprated_file = 'toprated_movies.snappy.parquet'
   upcoming_file = 'upcoming_movies.snappy.parquet'
   trending_file = 'trending_movies.snappy.parquet'
   current_file = 'current_weather.snappy.parquet'
   file_forecast = 'forecast.snappy.parquet'
   RATING_PATH = DATALAKE_ROOT_FOLDER + "formatted/movies/imdb/" + current_day + "/" + toprated_file
   UPCOMING_PATH = DATALAKE_ROOT_FOLDER + "formatted/movies/imdb/" + current_day + "/" + upcoming_file
   TRENDING_PATH = DATALAKE_ROOT_FOLDER + "formatted/movies/imdb/" + current_day + "/" + trending_file
   CURRENT_PATH = DATALAKE_ROOT_FOLDER + "formatted/weather/weatherapi/" + current_day + "/" + current_file
   FORECAST_PATH = DATALAKE_ROOT_FOLDER + "formatted/weather/weatherapi/" + current_day + "/" + file_forecast
   USAGE_OUTPUT_FOLDER_STATS = DATALAKE_ROOT_FOLDER + "usage/movieAnalysis/MovieStatistics/" + current_day + "/"
   USAGE_OUTPUT_FOLDER_BEST = DATALAKE_ROOT_FOLDER + "usage/movieAnalysis/TopMovies/" + current_day + "/"
   USAGE_OUTPUT_FOLDER_BEST2 = DATALAKE_ROOT_FOLDER + "usage/movieAnalysis/TopUpcoming/" + current_day + "/"
   USAGE_OUTPUT_FOLDER_BEST3 = DATALAKE_ROOT_FOLDER + "usage/movieAnalysis/TopTrending/" + current_day + "/"
   USAGE_OUTPUT_FOLDER_WEATHER = DATALAKE_ROOT_FOLDER + "usage/WeatherAnalysis/Current_Weather/" + current_day + "/"
   USAGE_OUTPUT_FOLDER_WEATHER2 = DATALAKE_ROOT_FOLDER + "usage/WeatherAnalysis/Forecast_Weather/" + current_day + "/"
   if not os.path.exists(USAGE_OUTPUT_FOLDER_STATS):
       os.makedirs(USAGE_OUTPUT_FOLDER_STATS)
   if not os.path.exists(USAGE_OUTPUT_FOLDER_BEST):
       os.makedirs(USAGE_OUTPUT_FOLDER_BEST)
   if not os.path.exists(USAGE_OUTPUT_FOLDER_BEST3):
       os.makedirs(USAGE_OUTPUT_FOLDER_BEST3)
   if not os.path.exists(USAGE_OUTPUT_FOLDER_WEATHER):
       os.makedirs(USAGE_OUTPUT_FOLDER_WEATHER)
   if not os.path.exists(USAGE_OUTPUT_FOLDER_WEATHER2):
       os.makedirs(USAGE_OUTPUT_FOLDER_WEATHER2)

   sc = SparkContext(appName="CombineData")
   sqlContext = SQLContext(sc)

   df_toprated = sqlContext.read.parquet(RATING_PATH)
   df_toprated.registerTempTable("toprated")

   df_upcoming = sqlContext.read.parquet(UPCOMING_PATH)
   df_upcoming.registerTempTable("upcoming")

   df_trending = sqlContext.read.parquet(TRENDING_PATH)
   df_trending.registerTempTable("trending")

   df_weather = sqlContext.read.parquet(CURRENT_PATH)
   df_weather.registerTempTable("weather")

   df_forecast = sqlContext.read.parquet(FORECAST_PATH)
   df_forecast.registerTempTable("forecast")



   toprated_analysis_df = sqlContext.sql("SELECT AVG(vote_count) AS avg_rating,"
                                    "       MAX(vote_count) AS max_rating,"
                                    "       MIN(vote_count) AS min_rating,"
                                    "       COUNT(vote_count) AS count_rating"
                                    "    FROM toprated"
                                    "    LIMIT 10")
   toprated_df = sqlContext.sql("SELECT title, popularity,vote_count"
                             "    FROM toprated"
                             "    ORDER BY vote_count DESC "
                             "    LIMIT 10")
   upcoming_df = sqlContext.sql("SELECT title, popularity,release_date,vote_count"
                                "    FROM upcoming"
                                "    ORDER BY release_date DESC "
                                )
   trending_df = sqlContext.sql("SELECT title, popularity, release_date, vote_count"
                                "    FROM upcoming"
                                "    ORDER BY vote_count DESC "
                                )

   weather_df = sqlContext.sql("SELECT temp_c"
                                "    FROM weather"
                                )
   forecast_df = sqlContext.sql("SELECT date, day.maxtemp_c as Maximum_Temperature, day.mintemp_c as Minimum_Temperature FROM forecast"
                                )
   f_m = sqlContext.sql("SELECT DISTINCT u.title, f.date, u.release_date, f.day.maxtemp_c as Maximum_Temperature, f.day.mintemp_c as Minimum_Temperature "
                        "FROM forecast f, upcoming u WHERE u.release_date=f.date"
                                )
    #Save the analysis in a file
   toprated_df.write.save(USAGE_OUTPUT_FOLDER_BEST + "res.snappy.parquet", mode="overwrite")
   upcoming_df.write.save(USAGE_OUTPUT_FOLDER_BEST2 + "res.snappy.parquet", mode="overwrite")
   trending_df.write.save(USAGE_OUTPUT_FOLDER_BEST3 + "res.snappy.parquet", mode="overwrite")
   toprated_analysis_df.write.save(USAGE_OUTPUT_FOLDER_STATS + "res.snappy.parquet", mode="overwrite")


   weather_df.write.save(USAGE_OUTPUT_FOLDER_WEATHER + "res.snappy.parquet", mode="overwrite")
   forecast_df.write.save(USAGE_OUTPUT_FOLDER_WEATHER2 + "res.snappy.parquet", mode="overwrite")

