import json
import os
from datetime import date
import pyarrow.parquet as pq
from elasticsearch import Elasticsearch
HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/datalake/"

def index_data_to_elasticsearch():
    current_day = date.today().strftime("%Y%m%d")
    RATING_PATH = DATALAKE_ROOT_FOLDER + "usage/movieAnalysis/TopMovies/" + current_day  +"/res.snappy.parquet/"
    ANALYSIS_PATH = DATALAKE_ROOT_FOLDER + "usage/movieAnalysis/MovieStatistics/" + current_day  +"/res.snappy.parquet/"
    UPCOMING_PATH = DATALAKE_ROOT_FOLDER + "usage/movieAnalysis/TopUpcoming/" + current_day  +"/res.snappy.parquet/"
    TRENDING_PATH = DATALAKE_ROOT_FOLDER + "usage/movieAnalysis/TopTrending/" + current_day  +"/res.snappy.parquet/"
    CURRENT_PATH = DATALAKE_ROOT_FOLDER + "usage/WeatherAnalysis/Current_Weather/" + current_day  +"/res.snappy.parquet/"
    FORECAST_PATH = DATALAKE_ROOT_FOLDER + "usage/WeatherAnalysis/Forecast_Weather/" + current_day  +"/res.snappy.parquet/"
    CLOUD_ID = "my_project:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvOjQ0MyQ1ODFjNzA0ZjRmY2Y0NzQ4OGYxZmViYjY3NDhlOThkYiRlZGFiNWI3NjEzMTc0MGYyYTY1MTYwMjE0ZjViZDEyYw=="
    ELASTIC_PASSWORD = "0ZkTh99itTYTKdL0WYDKaZCl"
        # Connect to Elasticsearch
    es = Elasticsearch(cloud_id=CLOUD_ID,basic_auth=("elastic", ELASTIC_PASSWORD))

    # Specify the index name
    analysis_index = "analysis_index"
    toprated_index = "toprated_index"
    upcoming_index = "upcoming_index"
    trending_index = "trending_index"
    current_index = "current_index"
    forecast_index = "forecast_index"

    # Read the contents of the file
    rated_table = pq.read_table(RATING_PATH)
    df_rated = rated_table.to_pandas()
    for _, row in df_rated.iterrows():
        document = row.to_dict()
        es.index(index=toprated_index, body=document)

    analysis_table = pq.read_table(ANALYSIS_PATH)
    df_analysis = analysis_table.to_pandas()
    for _, row in df_analysis.iterrows():
        document = row.to_dict()
        es.index(index=analysis_index, body=document)

    upcoming_table = pq.read_table(UPCOMING_PATH)
    df_upcoming = upcoming_table.to_pandas()
    for _, row in df_upcoming.iterrows():
        document = row.to_dict()
        es.index(index=upcoming_index, body=document)

    trending_table = pq.read_table(TRENDING_PATH)
    df_trending = trending_table.to_pandas()
    for _, row in df_trending.iterrows():
        document = row.to_dict()
        es.index(index=trending_index, body=document)

    current_table = pq.read_table(CURRENT_PATH)
    df_current = current_table.to_pandas()
    for _, row in df_current.iterrows():
        document = row.to_dict()
        es.index(index=current_index, body=document)

    forecast_table = pq.read_table(FORECAST_PATH)
    df_forecast = forecast_table.to_pandas()
    for _, row in df_forecast.iterrows():
        document = row.to_dict()
        es.index(index=forecast_index, body=document)
