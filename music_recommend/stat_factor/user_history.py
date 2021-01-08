from utils import UserDataApp, user_pre_db, movie_original_db

spark = UserDataApp().spark


def get_user_history(history_type):
    """
    :param history_type: 'click' or 'top' or 'play'
    :return:
    """
    spark.sql('use {}'.format(user_pre_db))
    if history_type == 'play':
        tmp_df = spark.sql('select user_id, movie_id, cate_id, datetime, play_time from merge_action').where(
            'play_time > 0')
    else:
        tmp_df = spark.sql(
            'select user_id, movie_id, cate_id, {}, datetime from merge_action'.format(history_type)) \
            .where('{} = 1'.format(history_type)).drop('{}'.format(history_type))
        # tmp_df.show()
    import pyspark.sql.functions as fn
    # 对重复观看的电影过滤只留下时间最近的
    ret = tmp_df.groupby('user_id', 'movie_id') \
        .agg(fn.max('cate_id').alias('cate_id'), fn.max('datetime').alias('datetime'))

    # 添加电影标题，便于查看结果
    title_df = spark.sql('select id movie_id, title, year from {}.db_asset'.format(movie_original_db))
    ret = ret.join(title_df, on='movie_id', how='left')
    from pyspark.sql import Window
    # 根据时间戳对观看记录进行排序
    ret = ret.withColumn('sort_time', fn.row_number().over(
        Window.partitionBy('user_id', 'cate_id').orderBy(ret['datetime'].desc())))
    return ret