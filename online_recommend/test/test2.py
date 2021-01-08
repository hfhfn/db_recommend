# from movie_recall.cal_similar import load_movie_vector, similar_model_path
# from movie_recall.word2vec import channel_id, channel
# from utils import channelInfo
from utils.spark_app import MovieDataApp
from utils.save_tohive import RetToHive


spark = MovieDataApp().spark
# tmp = add_topic_field()
import numpy as np
import pyspark.sql.functions as fn
tmp = spark.sql("select * from user_portrait.action_topic_sort").limit(1000)
# tmp = spark.sql("select * from user_recall.user_history_click_1969").where('datetime is null')
tmp = tmp.groupby('cate_id', 'topic').agg(fn.count('user_id').alias('count2'))
def map(row):
    weight = np.log(row.count2 + 1)
    return row.cate_id, row.topic, round(float(weight), 8)
tmp.rdd.map(map).toDF(['cate_id', 'topic', 'weight']).show()
# tmp = spark.sql("select * from movie_portrait.topic_weights").select('movie_id')
# spark.sql("select * from factor.stat_play").printSchema()
# tmp = spark.sql("select * from factor.match_id")#.where('cid=1969')#.where('weight > 1000')
# tmp = spark.sql("select * from movie_recall.movie_filter_version_recall").show()
# tmp = spark.sql("select aid, title from movie.db_asset_source").limit(100)
# tmp2 = spark.sql("select id aid, cid, title from movie.db_asset")
# tmp.join(tmp2, (tmp.aid == tmp2.aid) & (tmp.title == tmp2.title), how='left').show()
# tmp.join(tmp2, on=['aid', 'title'], how='left').show()
# tmp.show()
# print(tmp.columns)
# print(type(tmp.count()))
# tmp = tmp.dropDuplicates(['id'])
# tmp = tmp.dropDuplicates(['aid'])
# print(tmp.count())
# print(tmp.distinct().count())
# import pyspark.sql.functions as fn
# tmp2 = tmp.groupby('ys_id').agg(fn.count('*').alias('num')).where('num > 1')
# tmp2.show(100)
# print(tmp2.dropDuplicates(['os_id']).count())
# tmp = tmp.join(tmp2, on='deviceid', how='inner')
# tmp2 = tmp.groupby('mac').agg(fn.count('deviceid').alias('num')).where('num > 1')
# tmp = tmp.join(tmp2, on='mac', how='inner')
# tmp = spark.sql("select * from movie_recall.movie_bkmeans")
# print(tmp.select('mac').distinct().count())
# tmp.agg(*[(1 - (fn.count(c) / fn.count('*'))).alias(c + '_missing') for c in tmp.columns]).show()
# tmp.printSchema()
# tmp.select('movie_id','group').where('group = 0').show(truncate=False)
# count = tmp.select('topic').distinct().count()
# print(count)
# tmp = spark.sql("select * from user_portrait.user_profile").where('cate_id = 1969')
# tmp.show(100)
# print(tmp.count())
# tmp.describe('row_number').show()

#调用函数并起一个别名
# tmp.groupby('user_id').agg(fn.count('row_number').alias('count')).show()
# tmp.agg(fn.max('row_number').alias('max'),fn.mean('row_number').alias('mean'),fn.min('row_number').alias('min')).show()
# tmp = spark.sql("select * from movie_portrait.topic_weights")



import pyspark.sql.functions as fn
# tmp.agg(*[(1 - (fn.count(c) / fn.count('*'))).alias(c + '_missing') for c in tmp.columns]).show()
# spark.sql("DROP TABLE IF EXISTS {}".format(''))
# tmp.where('movie_len is null').fillna(-10, ['movie_len']).show()
# tmp.where('movie_len is null').fillna({'movie_len':-10}).show()
# tmp1 = tmp.count()
# tmp2 = tmp.dropna().count()
# print(tmp1, tmp2)



# print(merge_ret)
# merge_ret = spark.sql("select * from pre_user.merge_play")

# merge_ret1 = spark.sql("select * from pre_user.merge_play")
# merge_ret2 = spark.sql("select * from pre_user.merge_click")
# merge_ret3 = spark.sql("select * from pre_user.merge_top")
# print(merge_ret2.count(), merge_ret3.count())   3621450 2865676
# tmp  = merge_ret2.join(merge_ret3, on=["user_id", "movie_id", "datetime", 'cate_id'], how='inner')
# print(tmp.count())    # 6172545   7423374
# tmp.show()

# tmp.show(truncate=False)
# tmp.withColumn('test', tmp.ymd / (1000 * 60)).show(truncate=False)

# merge_ret = merge_ret.withColumn('play_time2', merge_ret.play_time) \
#     .drop('play_time').withColumnRenamed("play_time2", "play_time")
# merge_ret = merge_ret.where('play_time > 1')
# RetToHive(spark, merge_ret, 'pre_user', 'merge_play')

# merge_ret.show(100)


# def find(row):
#     if row.cate_id not in channelInfo:
#         return row.user_id, row.movie_id, row.cate_id, row.datetime
#
# ret = tmp.rdd.map(find).toDF(['user_id', 'movie_id', 'cate_id', 'datetime'])
# ret.show()

# import happybase
#
# pool = happybase.ConnectionPool(size=3, host="hadoop-master", table_prefix='movie', table_prefix_separator=b':')
#
# with pool.connection() as conn:
#
#     # conn.create_table('movie_lsh', {'similar': dict(max_versions=3)})
#     # conn.delete_table('movie_lsh', True)
#     table = conn.table('movie_lsh')
#     # for key, value in table.scan(row_start='1000', row_stop='1100'):
#     n = 0
#     for key, value in table.scan():
#         n += 1
#         k = 0
#         for i in value:
#             # print(key, i)
#             k += 1
#             # count = table.counter_dec(key.decode(), i.decode())
#             # print(count)
#         print(k)
#     print(n)
#     conn.close()


