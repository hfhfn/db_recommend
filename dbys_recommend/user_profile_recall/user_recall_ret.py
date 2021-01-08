from utils import u_topK, pre_topK, factor_db, movie_portrait_db


class UserRecall(object):

    def __init__(self, user_portrait_db, user_recall_db, spark, cate_id):
        self.spark = spark
        self.cate_id = cate_id
        self.topK = u_topK
        self.pre_topK = pre_topK
        self.user_portrait_db = user_portrait_db
        self.user_recall_db = user_recall_db
        self.factor_db = factor_db
        self.movie_protrait_db = movie_portrait_db

    def get_pre_recall(self):
        """
        join 倒排表和用户画像
        :param :
        :return:
        """
        user_profile = self.spark.sql("select * from {}.user_profile".format(self.user_portrait_db)) \
            .where('cate_id = {}'.format(self.cate_id)).drop('row_number')
        # 倒排表中存在不同topic词对应的电影列表中包含相同电影的情况， 需要在用户召回时做去重， 留下权重大的电影
        inverted_table = self.spark.sql(
            "select topic, movie_li from {}.inverted_table_{}".format(self.user_recall_db, self.cate_id))

        tmp = user_profile.join(inverted_table, on='topic', how='left').drop('topic')
        # tmp.show()
        # tmp.printSchema()

        return tmp

    def get_pre2_recall(self):
        """
        综合 用户画像权重和召回电影权重
        :return:
        """
        pre_recall = self.spark.sql("select * from {}.pre_user_recall_{}".format(self.user_recall_db, self.cate_id))

        def _map(partition):
            # i = 0
            global false, null, true
            # eval在python语言中不能识别小写的false和true还有null
            false = null = true = ''
            for row in partition:
                try:
                    # 因为 movie_li 有 1971 个 None，加try解决eval()出错
                    movie_li = eval(row.movie_li)
                    for key, value in movie_li:
                        yield row.user_id, row.cate_id, key, round(float(row.weight * value), 4)
                except:
                    # i += 1
                    # print('{}'.format(i) * 20)
                    # print(row.movie_li)
                    pass

        recall_df = pre_recall.rdd.mapPartitions(_map).toDF(['user_id', 'cate_id', 'movie_id', 'weight']).dropna()

        return recall_df

    def get_pre3_recall(self):
        """
        因为不同的电影画像词可能召回相同的电影， 需要去重，留下权重大的那个
        :param cate_id:
        :return:
        """
        pre2_recall = self.spark.sql("select * from {}.pre2_user_recall_{}".format(self.user_recall_db, self.cate_id))

        import pyspark.sql.functions as fn
        from pyspark.sql import Window
        recall_df = pre2_recall.withColumn("total_sort",
                                           fn.row_number().over(
                                               Window.partitionBy("user_id").orderBy(pre2_recall["weight"].desc())))

        """
        # 此方法是用 分组+join 区别相同电影最大权重的那个  （已弃用）
        # tmp_df = recall_df.groupby('user_id', 'movie_id').agg(fn.min('total_sort').alias('base_sort'))\
        #         .withColumnRenamed('user_id', 'uid').withColumnRenamed('movie_id', 'mid')
        # recall_df = recall_df.join(tmp_df, (recall_df.user_id==tmp_df.uid) & (recall_df.movie_id==tmp_df.mid),
        #                            how='left').drop('uid', 'mid')
        import numpy as np
        # 对于不同画像词召回的相同电影，基于所有召回的电影排名，对同一个电影排名第一的不做权重衰减，
        # 排名靠后的的权重做对数衰减
        # 然后再综合召回的同一个电影的所有权重，统一把权重聚合在一起，得到被召回多个相同电影的组合权重
        def map(row):
            if row.total_sort == row.base_sort:
                weight = row.weight
            else:
                weight = row.weight / (np.logs(row.total_sort) + 1)
            return row.user_id, row.cate_id, row.movie_id, weight
        """
        return recall_df

    def get_recall_ret(self):
        """
        因为不同的电影画像词可能召回相同的电影， 需要去重，留下权重大的那个
        :param cate_id:
        :return:
        """
        pre3_recall = self.spark.sql("select * from {}.pre3_user_recall_{}".format(self.user_recall_db, self.cate_id))
        import pyspark.sql.functions as fn
        from pyspark.sql import Window
        # 此方法是用 窗口函数排序 令 sort_num=1 区别相同电影权重最大的那个
        recall_df = pre3_recall.withColumn("sort_num",
                                           fn.row_number().over(Window.partitionBy("user_id", "movie_id").orderBy(
                                               pre3_recall["weight"].desc())))
        tmp_df = recall_df.where('sort_num=1').select('user_id', 'movie_id', 'total_sort') \
            .withColumnRenamed('total_sort', 'base_sort').withColumnRenamed('user_id', 'uid') \
            .withColumnRenamed('movie_id', 'mid')
        recall_df = recall_df.join(tmp_df, (recall_df.user_id == tmp_df.uid) & (recall_df.movie_id == tmp_df.mid),
                                   how='left').drop('uid', 'mid')
        import numpy as np
        # 对于不同画像词召回的相同电影，基于所有召回的电影排名，对同一个电影排名第一的不做权重衰减，
        # 排名靠后的的权重做对数衰减
        # 然后再综合召回的同一个电影的所有权重，统一把权重聚合在一起，得到被召回多个相同电影的组合权重
        def map(row):
            if row.sort_num == 1:
                weight = row.weight
            else:
                weight = row.weight / (np.log(row.total_sort - row.base_sort + 1) + 1)
            return row.user_id, row.cate_id, row.movie_id, round(float(weight), 4)

        from pyspark import StorageLevel
        recall_df = recall_df.rdd.persist(StorageLevel.MEMORY_AND_DISK).map(map).toDF(
            ['user_id', 'cate_id', 'movie_id', 'weight'])
        import pyspark.sql.functions as fn
        recall_df = recall_df.groupby('user_id', 'cate_id', 'movie_id').agg(fn.sum('weight').alias('weight'))

        """
        # 对于不同画像词召回的相同电影，只留权重最大的一个  (已舍弃， 以前用的去重策略)
        recall_df = pre2_recall.withColumn("sort_num",
                                           fn.row_number().over(Window.partitionBy("user_id", "movie_id").orderBy(
                                               pre2_recall["weight"].desc()))).where('sort_num = 1').drop('sort_num')
        # 以下4行为一种去重方法，已舍弃
        recall_df = pre2_recall.withColumn("sort_num",fn.row_number().over(Window.partitionBy("user_id")
                                   .orderBy(pre2_recall["weight"].desc()))).where('sort_num = 1')
        # 针对倒排表重复电影进行去重，留下权重大的电影， 排序后第一个就是权重大的， 去重会留下第一条出现的数据
        recall_df = recall_df.dropDuplicates(['user_id', 'cate_id', 'movie_id'])
        """
        return recall_df

    def get_tmp_recall_topK(self):
        recall_ret = self.spark.sql("select * from {}.user_recall_{}".format(self.user_recall_db, self.cate_id))
        import pyspark.sql.functions as fn
        from pyspark.sql import Window
        recall_ret = recall_ret.withColumn("sort_num",
                                           fn.row_number().over(
                                               Window.partitionBy("user_id").orderBy(recall_ret["weight"].desc())))
        recall_topK = recall_ret.where('sort_num <= {}'.format(self.pre_topK))

        return recall_topK



if __name__ == '__main__':
    pass

# import pandas as pd
#
# def _map_to_pandas(rdds):
#     """
#     Needs to be here due to pickling issues
#     """
#     return [pd.DataFrame(list(rdds))]
#
# def toPandas(df, n_partitions=None):
#     """
#     Returns the contents of `df` as a local `pandas.DataFrame` in a speedy fashion. The DataFrame is
#     repartitioned if `n_partitions` is passed.
#     :param df:              pyspark.sql.DataFrame
#     :param n_partitions:    int or None
#     :return:                pandas.DataFrame
#     """
#     if n_partitions is not None: df = df.repartition(n_partitions)
#     df_pand = df.rdd.mapPartitions(_map_to_pandas).collect()
#     df_pand = pd.concat(df_pand)
#     df_pand.columns = df.columns
#     return df_pand
#
# def get_recall_ret(cate_id):
#     # spark.sparkContext().addPyFile('../utils/spark_app.py')
#     user_profile = spark.sql("select * from user_portrait.user_profile")\
#                         .where('cate_id = {}'.format(cate_id)).drop('row_number')
#     inverted_table = spark.sql("select topic, movie_li from user_recall.inverted_table_{}".format(cate_id))
#
#     tmp = user_profile.join(inverted_table, on='topic', how='left').drop('topic')
#     # tmp.show()
#     recall_df = tmp.toPandas()
#     # recall_df = toPandas(tmp, 16)
#     print(recall_df.head())
#     recall_dict = {}
#     for i in range(len(recall_df)):
#         user_id = recall_df['user_id'][i]
#         movie_li = eval(recall_df['movie_li'][i])
#         weight = recall_df['weight'][i]
#         _ = recall_dict.get(user_id, [])
#         _ += [(key, value * weight) for key, value in movie_li]
#         recall_dict.setdefault(user_id, _)
#
#     recall_list = []
#     for key, value in recall_dict.items():
#         recall_list.append((key, str(sorted(value, key=lambda x: x[1], reverse=True)), len(value)))
#
#     from pyspark.sql import functions
#     recall_ret = spark.createDataFrame(recall_list, ['user_id', 'movie_li', 'movie_num'])
#     recall_ret = recall_ret.withColumn('cate_id', functions.lit(cate_id))
#
#     return recall_ret
