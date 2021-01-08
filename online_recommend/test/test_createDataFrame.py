import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE_DIR))

from pyspark import Row
from pyspark.sql.types import StringType
import pyspark.sql.functions as fn
from pyspark.sql import Window
from utils.default import user_pre_db
from utils.spark_app import MovieDataApp
from utils.save_tohive import RetToHive

spark = MovieDataApp().spark
sc = spark.sparkContext

# d = [
#     Row(a=1, intlist=['你'], mapfield={"a": "b"}),
#     Row(a=1, intlist=[3, 4], mapfield={}),
#     Row(a=1, intlist=[1, 2], mapfield={}),
#     Row(a=1, intlist=None, mapfield=None),
# ]
# rdd = sc.parallelize(d)
# data = spark.createDataFrame(rdd)
# # data.show()

# recall_df = data.withColumn('word', fn.explode('intlist'))
# recall_df.show()


# d = [
#     Row(user_id=1, movie_id=1, cate_id=1, weight=1, title=['《美国队长2普通话版']),
#     Row(user_id=1, movie_id=1, cate_id=1, weight=1, title=['《美国队长2普通话版']),
#     Row(user_id=1, movie_id=1, cate_id=1, weight=2, title=['美国队长2(普通话版)']),
#     Row(user_id=1, movie_id=1, cate_id=1, weight=5, title=['美国·队长普通话版']),
#     Row(user_id=2, movie_id=1, cate_id=1, weight=3, title=['美国队长3普通话版']),
#     Row(user_id=2, movie_id=1, cate_id=1, weight=4, title=['《']),
#     Row(user_id=2, movie_id=1, cate_id=1, weight=4, title=['《'])
# ]
#
# rdd = sc.parallelize(d)
# data = spark.createDataFrame(rdd)
#
# data.groupby('user_id', 'movie_id', 'cate_id').agg(fn.max('weight').alias('weight')).show()

# data.dropDuplicates()
# data.show()
# data = data.dropDuplicates()
# data.show()

# data = data.withColumn('sort_num', fn.row_number().over(
#     Window.partitionBy('movie_id').orderBy(data['user_id'], data['weight'].desc())))
# # data.printSchema()
# # data = data.withColumn("title", data.title.cast(StringType()))
# data.show()
# # data.printSchema()
#
# RetToHive(spark, data, 'test', 'insert')

# d2 = [
#     # Row(user_id=1, movie_id=1, test='haha'),
#     # Row(user_id=2, movie_id=1, test='hello')
#     Row(user_id=1, movie_id=1, cate_id=2, weight=3, title=['中国崛起']),
#     Row(user_id=2, movie_id=1, cate_id=1, weight=5, title=['创世纪'])
# ]
# rdd2 = sc.parallelize(d2)
# data2 = spark.createDataFrame(rdd2)

# # spark.sql('use movie')
# data2.write.insertInto("test.insert_test", overwrite=True)
# spark.sql('alter table insert rename to insert_test')
# data2.registerTempTable("temptable")
# spark.sql('INSERT into TABLE insert_test SELECT * FROM temptable WHERE weight > 3')
# spark.sql('INSERT overwrite TABLE online.movie_recall_1969_100 SELECT movie_id, title, movie_id2, title2, sort_num, timestamp FROM movie_recall.movie_recall_1969_100')
# spark.sql('create TABLE db_asset_update SELECT * FROM db_asset')
# spark.sql('select * from db_asset_update').show()

# 需要复制jar包到jdk中， cp mysql-connector-java-8.0.18.jar /home/local/jdk/jre/lib/ext
# jdbcDF = spark.read.format("jdbc").option("url","jdbc:mysql://mysql:3306/movie").option("dbtable","db_asset")\
#                 .option("user","root").option("password","mysql").load()
# jdbcDF.show()
# ret = data.join(data2, on=(['user_id', 'movie_id']), how='inner')
# ret.show()

# time = spark.sql('select * from movie.db_asset_update').agg(fn.max('create_time').alias('update_point'))\
#                                     .first().update_point
# print(type(time))

arr = [[95, 77, 48, 87, 46, 72, 62, 35, 93, 33],
           [96, 36, 28, 38, 40, 35, 7, 25, 42, 6],
           [97, 33, 58, 2, 12, 22, 0, 49, 35, 60],
           [98, 62, 35, 28, 72, 22, 52, 27, 69, 44],
           [99, 43, 75, 83, 22, 49, 72, 46, 62, 53]]

arr2 = [[95, 77, 48, 87, 46, 72, 62, 35, 93, 33],
           [96, 36, 28, 38, 40, 35, 7, 25, 42, 6],
           [97, 33, 58, 2, 12, 22, 0, 49, 35, 60],
           [98, 62, 35, 28, 72, 22, 52, 27, 69, 44],
           [99, 43, 75, 83, 22, 49, 72, 46, 62, 53]]
import numpy as np
# arr = np.asarray(arr)#.astype('float32')
# arr2 = np.asarray(arr2)#.astype('float32')
import pandas as pd
# df = pd.DataFrame(arr, index=['row1', 'row2'], columns=['c1', 'c2', 'c3'])
# df = pd.DataFrame(arr).add_prefix('index_')
# df2 = pd.DataFrame(arr2).add_prefix('sim_')
# concat_df = pd.concat([df, df2], axis=1)
# # print(df)
# print(concat_df.loc[concat_df.index[0], 'index_0'])
# sp_df = spark.createDataFrame(df)
# sp_df.show()

df = pd.DataFrame(columns=['index_'])
df.append(pd.Series([None]), ignore_index=True)
df['index_'] = arr
df2 = pd.DataFrame(columns=['similar'])
df2.append(pd.Series([None]), ignore_index=True)
df2['similar'] = arr2  # 欧式距离

concat_df = pd.concat([df, df2], axis=1)
# # print(concat_df)
sp_df = spark.createDataFrame(concat_df)
sp_df.show()

def map(partition):
    for row in partition:
        for i in range(10):
            cos_sim = row.similar[i]
            movie_id2 = row.index_[i] + i
            if cos_sim == movie_id2:
                # print(cos_sim)
                continue
            yield movie_id2, cos_sim
#
recall_df = sp_df.rdd.mapPartitions(map).toDF(['movie_id2', 'cos_sim'])
recall_df.show()
# print(recall_df.count())

# movie_df = spark.sql("select id, cid, title from movie.db_asset").where('cid={}'.format(1969)).drop('cid')
# movie_df2 = spark.sql("select id, title from movie.db_asset").where('cid={}'.format(1969))
#
# print(movie_df.count(), movie_df2.count())

# title_db = spark.sql("select id movie_id, title from movie.db_asset")
# tmp_table = spark.sql("select * from pre_user.merge_play")
# tmp_table = tmp_table.join(title_db, on='movie_id', how='left')
# RetToHive(spark, tmp_table, 'test', 'merge_play')


"""
查询过滤后的少儿片源
"""
# 去除无cate标签的少儿影片, 以及去除title大于16的影片
movie_original_db = 'movie'
se_df = spark.sql("select id movie_id2, title from {}.db_asset where cid = 1973".format(movie_original_db)).where('cate != "" and length(title) <= 16')

# 去除时长小于60分钟的 单剧集的影片
eptotal_df = spark.sql("select aid movie_id2, eptotal from {}.db_asset_source \
        where source = 12 and eptotal = 1 and get_json_object(extra, '$.len')/60 < 60".format(movie_original_db))
recall_ret = se_df.join(eptotal_df, on='movie_id2', how='left').where('eptotal is null').drop('eptotal')

print(recall_ret.count())
recall_ret.show()