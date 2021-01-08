from utils import m_spark, get_filter_data


def pre_movie_data():
    """
    预处理电影数据
    :return: 【dataframe】
    """
    # spark.catalog.listDatabases()

    movie_data = m_spark.sql("select * from movie.db_asset")
    # count = movie_data.count()
    # tmp = movie_data.head()
    # movie_data = movie_data.where("cid=1969")
    # movie_data.show()
    # print(movie_data.count())

    movie_data = get_filter_data(movie_data)
    from pyspark.sql.functions import concat_ws

    # movieid和各个特征合并
    # sql = '''
    # select id, cid, concat_ws(",", collect_set(fir_movie)) as summary
    # (select title, desc, cate, year, area, actor, director, score, all_count, update_time from movie_data as fir_movie)
    # '''
    # # 选取movie表中需要保留的数据
    # spark.sql(sql).show()

    fir_movie = movie_data.select("id", "cid", "title", "create_time", \
                                  concat_ws( \
                                      ",", \
                                      movie_data.title, \
                                      movie_data.cate, \
                                      movie_data.actor, \
                                      movie_data.director, \
                                      movie_data.area, \
                                      movie_data.desc, \
                                      ).alias("summary")
                                  )
    # fir_movie.show()
    # import pyspark.sql.functions as fn
    # from pyspark.sql import Window
    #
    # sec_movie = fir_movie.withColumn("row_number",
    #                                  fn.row_number().over(Window.partitionBy("cid").orderBy(fir_movie["id"].asc())))
    # fir_movie.withColumn("row_number", fn.row_number().over(Window.partitionBy("cid").orderBy(fn.desc('id')))).show()

    # sec_movie.show()

    return fir_movie


# pre_movie_data().registerTempTable("tempTable")
# # 查看临时表结构
# spark.sql("desc tempTable").show()


if __name__ == '__main__':
    df = pre_movie_data()
    # df.show()
    print(df.count())
