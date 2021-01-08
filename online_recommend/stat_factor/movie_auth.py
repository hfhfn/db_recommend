"""
过滤auth字段，选取非预告片 （0：正片，1：预告片，2：vip, 3:单点，4：用券）
"""

from utils.spark_app import MovieDataApp


spark = MovieDataApp().spark

def get_movie_auth():
    movie_data = spark.sql("select aid, title, auth from movie.db_asset_source where auth = 1 and source = 12")

    return movie_data
