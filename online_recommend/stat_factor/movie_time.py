from utils.spark_app import MovieDataApp


spark = MovieDataApp().spark

def get_movie_time():
    movie_data = spark.sql("select * from movie.db_asset_source")

    def extract_movie_time(row):
        movie_len = None
        try:
            tmp_dict = eval(row.extra)
            # print(tmp_dict)
            movie_len = tmp_dict.get('len')
            if movie_len is not None:
                movie_len = int(int(movie_len) / 60)
        except:
            pass
        return row.aid, movie_len  # try eval出错时此处mivie_len返回None

    tmp_table = movie_data.rdd.map(extract_movie_time).toDF(['movie_id', 'movie_len']).dropna()
    import pyspark.sql.functions as fn
    from pyspark.sql import Window
    tmp_table = tmp_table.withColumn("sort_num",
                                     fn.row_number().over(Window.partitionBy("movie_id").orderBy(fn.asc('movie_len'))))
    # tmp_table.show()
    # 影视的时长有些是有两个，一个是所有剧集的总时长，一个是单集时长， 在这里取单集时长，即排序选小的那个时间
    tmp_table = tmp_table.where('sort_num=1').where('movie_len<200').drop('sort_num')

    return tmp_table