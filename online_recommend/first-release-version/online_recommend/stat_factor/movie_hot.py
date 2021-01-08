from utils import MovieDataApp, factor_db, movie_original_db

spark = MovieDataApp().spark


# def get_stat_play():
#     play_df = spark.sql("select last_play play, uptime from dbys.tp_record_play")
#     def _map(row):
#         global false, null, true
#         # eval在python语言中不能识别小写的false和true还有null
#         false = null = true = ''
#         albumId, aid, cid, totalTime, startTime = None, None, None, None, None
#         try:
#             tmp_dict = eval(row.play)
#             tmp_dict = tmp_dict.get('param')
#             if tmp_dict is not None:
#                 albumId = int(tmp_dict.get('albumId', -10))
#                 aid = int(tmp_dict.get('aid', -10))
#                 cid = int(tmp_dict.get('cid', -10))
#                 totalTime = int(tmp_dict.get('totalTime', -600000))/1000/60
#                 startTime = int(tmp_dict.get('startTime', -600000))/1000/60
#         except:
#             pass
#         return albumId, aid, cid, totalTime, startTime, row.uptime
#
#     ret = play_df.rdd.map(_map).toDF(['qid', 'aid', 'cid', 'totalTime', 'playtime', 'timestamp'])
#     ret = ret.distinct().dropna()
#     return ret
#
#
# def stat_hot_count(df):
#     def filter_play(row):
#         score = 0
#         playtime = row.playtime
#         totaltime = row.totalTime
#         if playtime > 6:
#             if playtime <= totaltime:
#                 if playtime / totaltime > 0.5:
#                     score = 1
#             else:
#                 if playtime > 30:
#                     score = 1
#         return row.aid, row.cid, score
#
#     stat_play = df.rdd.map(filter_play).toDF(['aid', 'cid', 'score'])
#     import pyspark.sql.functions as fn
#     hot_weights = stat_play.groupby('aid', 'cid').agg(fn.sum('score').alias('weight'))
#
#     return hot_weights


# def get_movie_hot_sort():
#     import time
#     from datetime import timedelta
#     interval = timedelta(days=90)
#     timestamp = int(time.time()) - int(interval.total_seconds())
#     stat_play = spark.sql("select * from {}.movie_play".format(factor_db)).where('timestamp > {}'.format(timestamp))
#     hot_weights = stat_hot_count(stat_play)
#     match_id = spark.sql("select * from {}.match_id".format(factor_db))
#     hot_weights = hot_weights.join(match_id, (hot_weights.aid == match_id.ys_id) & (hot_weights.cid == match_id.os_cid),
#                                 how='left').drop('ys_id', 'aid', 'cid', 'title')
#     # hot_weights.show()
#
#     os_movie = spark.sql('select id aid, title, cid from movie.db_asset')
#     os_hot = os_movie.join(hot_weights, (os_movie.aid == hot_weights.os_id) & (os_movie.cid == hot_weights.os_cid),
#                            how='left').drop('os_id', 'os_cid')
#     def map(row):
#         weight = row.weight
#         if not weight:
#             weight = 0
#         return row.aid, row.title, row.cid, weight
#
#     os_hot = os_hot.rdd.map(map).toDF(['aid', 'title', 'cid', 'weight'])
#     import pyspark.sql.functions as fn
#     from pyspark.sql import Window
#     hot_sort = os_hot.withColumn("sort_num",
#                 fn.row_number().over(Window.partitionBy("cid").orderBy(fn.desc('weight'))))
#
#     return hot_sort      #   aid| title|cid|weight|sort_num|


def get_movie_hot_sort():
    import time
    from datetime import timedelta, datetime
    interval = timedelta(days=30)
    timestamp = int(time.time()) - int(interval.total_seconds())
    date_time = int(datetime.strftime(datetime.fromtimestamp(timestamp), "%Y%m%d"))
    stat_play = spark.sql("select aid, date, count from {}.ty_asset_play".format(movie_original_db)).where(
        'date > {}'.format(date_time))
    import pyspark.sql.functions as fn
    total_count = stat_play.groupby('aid').agg(fn.sum('count').alias('weight'))

    match_id = spark.sql("select id, cid, title from {}.db_asset".format(movie_original_db))
    hot_weights = match_id.join(total_count, total_count.aid == match_id.id, how='left').drop('aid')

    def map(row):
        weight = row.weight
        if not weight:
            weight = 0
        return row.id, row.title, row.cid, weight

    os_hot = hot_weights.rdd.map(map).toDF(['aid', 'title', 'cid', 'weight'])
    from pyspark.sql import Window
    hot_sort = os_hot.withColumn("sort_num",
                                 fn.row_number().over(Window.partitionBy("cid").orderBy(fn.desc('weight'))))

    return hot_sort  # aid| title|cid|weight|sort_num|


def get_movie_hot_factor():
    hot_df = spark.sql('select * from {}.movie_hot_sort'.format(factor_db))
    import pyspark.sql.functions as fn
    tmp = hot_df.agg(fn.max('weight').alias('max')).first()
    max = tmp.max
    print(max)

    def map(row):
        deltaScore = max - row.weight
        # 衰减因子
        import numpy as np
        # 电影行为次数，变化率比较大，范围宽，适合对数加权，可以降低热度低的电影权重的衰减速度
        hot_exp = 1 / (np.log(deltaScore + 1) + 1)
        return row.aid, row.title, row.cid, row.weight, round(float(hot_exp), 8), row.sort_num

    ret = hot_df.rdd.map(map).toDF(['aid', 'title', 'cid', 'play_num', 'factor', 'sort_num'])

    return ret


if __name__ == '__main__':
    # r = get_stat_play()
    # r.show(truncate=False)
    # print(r.count())
    # get_movie_hot_factor()

    pass
