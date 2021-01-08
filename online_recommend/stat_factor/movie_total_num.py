from utils.default import movie_original_db
from utils.spark_app import MovieDataApp
import pyspark.sql.functions as F


spark = MovieDataApp().spark

def get_movie_total_num():
    # 由于 db_asset_source 表中有内部和外部资源之分，而且有些剧集有多个版本，聚合选择最大的剧集
    total_num = spark.sql("select aid movie_id, eptotal total_num from {}.db_asset_source".format(
        movie_original_db)).groupby('movie_id').agg(F.max('total_num').alias('total_num'))

    return total_num
