from utils import MovieDataApp, movie_original_db

spark = MovieDataApp().spark

def get_movie_num_factor():
    # source_data = spark.sql("select * from movie.db_asset_source").select('aid', 'title', 'eptotal')\
    #                         .dropDuplicates(['aid', 'eptotal'])
    # import pyspark.sql.functions as fn
    # from pyspark.sql import Window
    # source_data = source_data.withColumn("sort_num",
    #            fn.row_number().over(Window.partitionBy("aid").orderBy(fn.desc('eptotal'))))\
    #             .where('sort_num = 1').drop('sort_num')

    import pyspark.sql.functions as F
    source_data = spark.sql("select aid movie_id, eptotal total_num from {}.db_asset_source".format(
        movie_original_db)).groupby('movie_id').agg(F.max('total_num').alias('total_num'))

    movie_data = spark.sql("select id movie_id, cid cate_id, title from {}.db_asset".format(movie_original_db))
    source_data = source_data.join(movie_data, on='movie_id', how='left')

    source_data.show(100)
    return source_data