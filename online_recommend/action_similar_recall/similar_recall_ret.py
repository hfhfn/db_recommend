from utils.default import u_topK, m_topK, movie_recall_db, factor_db, movie_original_db, u_spark


class UserSimilarRecall(object):

    def __init__(self, protrait_db, recall_db, spark, cate_id):
        self.spark = spark
        self.cate_id = cate_id
        self.u_topK = u_topK
        self.m_topK = m_topK
        self.protrait_db = protrait_db
        self.recall_db = recall_db
        self.history_db = factor_db

    def get_user_similar_recall(self):
        import gc

        action_weight = self.spark.sql(
            "select user_id, movie_id, cate_id, weight action_weight from {}.action_weight_normal"
                .format(self.protrait_db)).where('cate_id={}'.format(self.cate_id)).drop('cate_id')
        movie_recall = self.spark.sql(
            'select * from {}.movie_recall_{}_{}'.format(movie_recall_db, self.cate_id, self.m_topK))

        recall_df = action_weight.join(movie_recall, on='movie_id', how='left')
        del action_weight
        del movie_recall
        gc.collect()

        def map(row):
            try:
                weight = row.action_weight * row.weight
                return row.user_id, row.movie_id, row.title, row.year, row.movie_id2, row.title2, row.year2, row.score, \
                       row.play_num, round(float(weight), 8)
            except:
                return None, None, None, None, None, None, None, None, None, None
        # .repartition(500)
        recall_df = recall_df.rdd.map(map)
        recall_df = recall_df.toDF(['user_id', 'movie_id', 'title', 'year', 'movie_id2', 'title2',
                                                 'year2', 'score', 'play_num', 'weight']).dropna()

        import pyspark.sql.functions as fn
        from pyspark.sql import Window
        recall_df = recall_df.withColumn("total_sort", fn.row_number().over(
            Window.partitionBy("user_id").orderBy(fn.desc('weight'))))

        return recall_df

    def get_filter_same_recall(self):
        """
        因为不同的历史行为电影可能召回相同的电影， 需要去重
        :param cate_id:
        :return:
        """
        similar_recall = self.spark.sql("select * from {}.user_similar_recall_{}".format(self.recall_db, self.cate_id))
        import pyspark.sql.functions as fn
        from pyspark.sql import Window
        # 此方法是用 窗口函数排序 令 sort_num=1 区别相同电影权重最大的那个
        recall_df = similar_recall.withColumn("sort_num",
                                              fn.row_number().over(Window.partitionBy("user_id", "movie_id2").orderBy(
                                                  similar_recall["weight"].desc())))
        tmp_df = recall_df.where('sort_num=1').select('user_id', 'movie_id2', 'total_sort') \
            .withColumnRenamed('total_sort', 'base_sort').withColumnRenamed('user_id', 'uid') \
            .withColumnRenamed('movie_id2', 'mid')
        recall_df = recall_df.join(tmp_df, (recall_df.user_id == tmp_df.uid) & (recall_df.movie_id2 == tmp_df.mid),
                                   how='left').drop('uid', 'mid')
        import numpy as np
        # 对于不同历史观看电影召回的相同电影，基于所有召回的电影排名，对同一个电影排名第一的不做权重衰减，
        # 排名靠后的的权重做对数衰减
        # 然后再综合召回的同一个电影的所有权重，统一把权重聚合在一起，得到被召回多个相同电影的组合权重
        def map(row):
            if row.sort_num == 1:
                weight = row.weight
            else:
                weight = row.weight / (np.log(row.total_sort - row.base_sort + 1) + 1)
            return row.user_id, row.movie_id, row.title, row.year, row.movie_id2, row.title2, row.year2, row.score, \
                   row.play_num, round(float(weight), 8)

        recall_df = recall_df.rdd.map(map).toDF(['user_id', 'movie_id', 'title', 'year', 'movie_id2', 'title2',
                                                 'year2', 'score', 'play_num', 'weight'])
        recall_df.registerTempTable("temptable")
        merge_base_movie = self.spark.sql("select user_id, movie_id2 movie_id, title2 title, year2 year, score, \
            play_num, sum(weight) weight, collect_list(movie_id) base_movie, collect_list(title) base_title, \
            collect_list(year) base_year from temptable group by user_id, movie_id2, title2, year2, score, play_num")

        # 基于哪些用户行为的电影推荐的，合并成字典
        def _func(row):
            return row.user_id, row.movie_id, row.title, row.year, row.score, row.play_num, row.weight, list(
                zip(row.base_movie, row.base_title, row.base_year))

        recall_df = merge_base_movie.rdd.map(_func).toDF(
            ["user_id", "movie_id", "title", "year", "score", "play_num", "weight", "base_movie"])
        # import pyspark.sql.functions as fn
        # sort_df = recall_df.groupby('user_id', 'movie_id2').agg(fn.sum('weight').alias('weight'))

        return recall_df

    def get_filter_history_recall(self):
        self.spark.sql('use {}'.format(self.history_db))
        # 只过滤 用户播放和点击历史记录
        user_history_play = self.spark.sql('select user_id uid, movie_id mid from user_history_play') \
            .where('cate_id = {}'.format(self.cate_id))
        user_history_click = self.spark.sql('select user_id uid, movie_id mid from user_history_click') \
            .where('cate_id = {}'.format(self.cate_id))
        user_history = user_history_play.union(user_history_click).dropDuplicates(['uid', 'mid'])

        user_recall = self.spark.sql(
            'select * from {}.user_similar_filter_same_recall_{}'.format(self.recall_db, self.cate_id))
        ret = user_recall.join(user_history,
                               (user_recall.user_id == user_history.uid) & (user_recall.movie_id == user_history.mid),
                               how='left').where('mid is null').drop('uid', 'mid')
        """
        def del_history(row):
            if row.mid is not None:
                return None, None, None, None, None, None, None, None
            else:
                return row.user_id, row.movie_id, row.title, row.year, row.score, row.play_num, row.weight, \
                       str(row.base_movie)

        ret = ret.rdd.map(del_history).toDF(
            ['user_id', 'movie_id', 'title', 'year', 'score', 'play_num', 'weight', 'base_movie']).dropna()
        """
        import gc
        del user_history_play
        del user_history_click
        del user_recall
        del user_history
        gc.collect()

        # import time
        # import datetime
        # today = datetime.date.today()
        # day_timestamp = int(today.strftime('%Y%m%d'))
        # # day_timestamp = int(time.mktime(time.strptime(str(today), '%Y-%m-%d')))  # 零点时间戳
        #
        # import pyspark.sql.functions as fn
        # from pyspark.sql import Window
        # ret = ret.withColumn("sort_num", fn.row_number().over(
        #     Window.partitionBy("user_id").orderBy(ret["weight"].desc()))).withColumn('timestamp', fn.lit(day_timestamp))
        return ret

    def get_user_similar_latest_recall(self):
        user_recall = self.spark.sql(
            'select * from {}.user_similar_filter_history_recall_{}'.format(self.recall_db, self.cate_id))
        from pyspark.sql.types import StringType
        user_recall = user_recall.withColumn("base_movie", user_recall.base_movie.cast(StringType()))
            # .where('sort_num <= {}'.format(self.u_topK))

        movie_source = self.spark.sql("select aid movie_id, source from {}.db_asset_source".format(movie_original_db))\
                    .where('source = 12')

        ret = user_recall.join(movie_source, on='movie_id', how='left').where('source = 12').drop('source')

        import gc
        del user_recall
        del movie_source
        gc.collect()

        # def source_map(row):
        #     # source字段 为12 的是内部资源
        #     if row.source == 12:
        #         source_num = 0
        #     else:
        #         source_num = 1
        #     return  row.user_id, row.movie_id, row.title, row.year, row.score, \
        #             row.play_num, row.weight, row.base_movie, source_num
        #
        # ret = ret.rdd.map(source_map).toDF(
        #     ["user_id", "movie_id", "title", "year", "score", "play_num", "weight", "base_movie", "source_num"])

        import time
        import datetime
        today = datetime.date.today()
        day_timestamp = int(today.strftime('%Y%m%d'))
        # day_timestamp = int(time.mktime(time.strptime(str(today), '%Y-%m-%d')))  # 零点时间戳

        import pyspark.sql.functions as fn
        from pyspark.sql import Window
        ret = ret.withColumn("sort_num", fn.row_number().over(Window.partitionBy("user_id")\
                .orderBy(ret["weight"].desc()))).withColumn('timestamp', fn.lit(day_timestamp))\
                .where('sort_num <= {}'.format(self.u_topK))

        # ret = ret.withColumn("sort_num", fn.row_number().over(
        #     Window.partitionBy("user_id").orderBy(ret["source_num"], ret["weight"].desc())))\
        #     .withColumn('timestamp', fn.lit(day_timestamp))
        return ret


if __name__ == '__main__':
    UserSimilarRecall('update_user', 'update_user', u_spark, 1969).get_user_similar_latest_recall().show()