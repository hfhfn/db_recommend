from pyspark import Row
from pyspark.sql.types import StringType
import pyspark.sql.functions as fn
from pyspark.sql import Window
from utils import MovieDataApp, RetToHive, user_pre_db, user_portrait_db

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
#     Row(user_id=1, movie_id=1, cate_id=1, weight=2, title=['美国队长2(普通话版)']),
#     Row(user_id=1, movie_id=1, cate_id=1, weight=5, title=['美国·队长普通话版']),
#     Row(user_id=2, movie_id=1, cate_id=1, weight=3, title=['美国队长3普通话版']),
#     Row(user_id=2, movie_id=1, cate_id=1, weight=4, title=['《'])
# ]
#
# rdd = sc.parallelize(d)
# data = spark.createDataFrame(rdd)

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

# arr = [[95, 77, 48, 87, 46, 72, 62, 35, 93, 33],
#            [96, 36, 28, 38, 40, 35, 7, 25, 42, 6],
#            [97, 33, 58, 2, 12, 22, 0, 49, 35, 60],
#            [98, 62, 35, 28, 72, 22, 52, 27, 69, 44],
#            [99, 43, 75, 83, 22, 49, 72, 46, 62, 53]]
#
arr2 = [[95, 77, 48, 87, 46, 72, 62, 35, 93, 33],
           [96, 36, 28, 38, 40, 35, 7, 25, 42, 6],
           [97, 33, 58, 2, 12, 22, 0, 49, 35, 60],
           [98, 62, 35, 28, 72, 22, 52, 27, 69, 44],
           [99, 43, 75, 83, 22, 49, 72, 46, 62, 53]]
import numpy as np
# arr = np.asarray(arr)#.astype('float32')
# arr2 = np.asarray(arr2)#.astype('float32')
import pandas as pd
# arr = [[95, 77, 48],
#         [96, 36, 28]]
# df = pd.DataFrame(arr, index=['row1', 'row2'], columns=['c1', 'c2', 'c3'])
# print(df['c2']['row1'])
# df['c2']['row1']=10
# print(df['c2']['row1'])
# df = pd.DataFrame(arr, columns=['c1', 'c2', 'c3'])
# df1 = pd.DataFrame(arr).add_prefix('index_')
# print(df1.head())
# df2 = pd.DataFrame(arr2).add_prefix('sim_')
# concat_df = pd.concat([df, df2], axis=1)
# # print(df)
# print(concat_df.loc[concat_df.index[0], 'index_0'])
# sp_df = spark.createDataFrame(df).show()
# sp_df.show()

# df = pd.DataFrame(columns=['index_'])
# df.append(pd.Series([None]), ignore_index=True)
# df['index_'] = arr
# df2 = pd.DataFrame(index=range(10), columns=['similar', 'weight'])
# df2.append(pd.Series([None]), ignore_index=True)
# df2['similar'] = arr2[1]  # 欧式距离
# df2['weight'] = arr2[0]
# print(df2.head())
#
# concat_df = pd.concat([df, df2], axis=1)
# # print(concat_df)
# sp_df = spark.createDataFrame(concat_df)
# # sp_df.show()

# def map(partition):
#     for row in partition:
#         for i in range(10):
#             cos_sim = row.similar[i]
#             movie_id2 = row.index_[i] + i
#             if cos_sim == movie_id2:
#                 # print(cos_sim)
#                 continue
#             yield movie_id2, cos_sim
#
# recall_df = sp_df.rdd.mapPartitions(map).toDF(['movie_id2', 'cos_sim'])
# recall_df.show()
# print(recall_df.count())

# action_weight_ret = spark.sql('select * from update_user.action_weight')
# action_weight_table = 'action_weight'
# RetToHive(spark, action_weight_ret, user_portrait_db, action_weight_table)