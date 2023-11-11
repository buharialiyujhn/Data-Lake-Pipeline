import os
from datetime import date

import requests

HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/datalake/"
def fetch_data_from_imdb(**kwargs):
   current_day = date.today().strftime("%Y%m%d")
   TARGET_PATH = DATALAKE_ROOT_FOLDER + "raw/movies/imdb/" + current_day + "/"
   if not os.path.exists(TARGET_PATH):
       os.makedirs(TARGET_PATH)
   url = "https://api.themoviedb.org/3/movie/top_rated?language=en-US&page=1"
   url2 = "https://api.themoviedb.org/3/movie/upcoming?language=en-US&page=1"
   url3 = "https://api.themoviedb.org/3/movie/now_playing?language=en-US&page=1"

   headers = {
       "accept": "application/json",
       "Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiI4ZTI4Y2FjNmEyNGQwN2I0YTkzY2YwMWJlNzkzZDBhYSIsInN1YiI6IjY0NzM0YzgzZGQ3MzFiMmQ3OGI5OTViMSIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.98ClZjffymVV4M5FMdhj6ifk_nn8YG5PIl9WtOgqmXw"
   }
   headers2 = {
       "accept": "application/json",
       "Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiI4ZTI4Y2FjNmEyNGQwN2I0YTkzY2YwMWJlNzkzZDBhYSIsInN1YiI6IjY0NzM0YzgzZGQ3MzFiMmQ3OGI5OTViMSIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.98ClZjffymVV4M5FMdhj6ifk_nn8YG5PIl9WtOgqmXw"
   }
   headers3 = {
       "accept": "application/json",
       "Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiI4ZTI4Y2FjNmEyNGQwN2I0YTkzY2YwMWJlNzkzZDBhYSIsInN1YiI6IjY0NzM0YzgzZGQ3MzFiMmQ3OGI5OTViMSIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.98ClZjffymVV4M5FMdhj6ifk_nn8YG5PIl9WtOgqmXw"
   }

   response = requests.get(url, headers=headers)
   response2 = requests.get(url2, headers=headers2)
   response3 = requests.get(url3, headers=headers3)
   open(TARGET_PATH + 'toprated_movies.json', 'wb').write(response.content)
   open(TARGET_PATH + 'upcoming_movies.json', 'wb').write(response2.content)
   open(TARGET_PATH + 'trending_movies.json', 'wb').write(response3.content)