from utils import channelInfo, dim, user_portrait_db
from pyspark.ml.feature import Word2Vec
from pyspark.ml.feature import Word2VecModel


class Word2Vector(object):

    def __init__(self, spark, refit, channel='电影'):

        self.refit = refit
        self.channel = channel
        self.channel_id = channelInfo[self.channel]
        self.user_portrait_db = user_portrait_db
        self.dim = dim
        self.word2vec_model_path = "hdfs://hadoop-master:8020/movie/models/word2vec/channel_%d_%s.word2vec" \
                                   % (self.channel_id, self.channel)
        self.spark = spark
        self.spark.sql("use movie_portrait")

    def cal_word2vec(self):
        # 训练词向量模型

        words_df = self.spark.sql("select * from cut_words where cate_id={}".format(self.channel_id))
        # words_df.show()
        # words_df = w2v.spark.sql("select movie_id, count(cate_id) cate from cut_words group by movie_id")
        # words_df.select(['movie_id', 'cate']).filter('cate > 1').show()

        # minCount：过滤次数默认小于5次的词
        word2Vec = Word2Vec(vectorSize=self.dim, inputCol="words", outputCol="model", minCount=3)
        wv_model = word2Vec.fit(words_df)
        wv_model.write().overwrite().save(self.word2vec_model_path)

    def load_word2vec(self):
        if self.refit:
            self.cal_word2vec()

        # from pyspark import SparkConf, SparkContext
        # SparkContext()
        # SparkConf().set("spark.storage.memoryFraction","0.6")

        # 加载某个频道模型，得到每个词的向量
        wv_model = Word2VecModel.load(self.word2vec_model_path)
        vectors = wv_model.getVectors()
        # show_vectors = vectors.head(10)
        # print(show_vectors)
        # vectors.show()
        # count = vectors.count()    # 77536
        # print(count)

        from pyspark.sql.functions import format_number as fmt  # 对数据格式化
        # wv_model.findSynonyms("北京", 20).select("word", fmt("similarity", 5).alias("similarity")).show()
        # wv_model.findSynonyms("科幻", 20).show()

        return vectors

    def get_movie_vector(self, update=None):

        # 增量转vector
        # import pyspark.sql.functions as fn
        # update_point = self.spark.sql('select * from db_asset_update').agg(fn.max('create_time').alias('update_point'))\
        #                         .first().update_point
        # 如果更新出现问题，可以使用上面这个时间戳作为过滤的节点（当前使用的用于过滤的时间戳是任务当天零点的时间戳）
        if update:
            topic_weight = update.where('cate_id = {}'.format(self.channel_id))
            # import time
            # import datetime
            # today = datetime.date.today()
            # day_timestamp = int(time.mktime(time.strptime(str(today), '%Y-%m-%d')))  # 零点时间戳
            # topic_weight = self.spark.sql("select * from topic_weights where cate_id={} and timestamp > {}"
            #                           .format(self.channel_id, day_timestamp))
        # 全量的电影画像权重，全量转vector
        else:
            topic_weight = self.spark.sql("select * from topic_weights where cate_id={}".format(self.channel_id))

        vectors = self.load_word2vec()
        # 合并计算（对每个词的向量进行权重加权）得到电影每个词的权重向量
        _movie_profile = topic_weight.join(vectors, vectors.word == topic_weight.topic, "inner")
        # rdd.repartition(100) 可以改变存储分区数量
        movieKeywordVectors = _movie_profile.rdd.map(lambda row: (row.movie_id, row.cate_id, row.topic,
                                                                  row.weight * row.vector)).toDF(
            ["movie_id", "cate_id", "topic", "weightingVector"])

        # 计算得到电影的平均词向量即电影的向量
        def avg(row):
            x = 0
            for v in row.vectors:
                x += v
            #  将平均向量作为movie的向量
            return row.movie_id, row.cate_id, x / len(row.vectors)

        movieKeywordVectors.registerTempTable("tempTable")
        movieVector = self.spark.sql(
            "select movie_id, min(cate_id) cate_id, collect_set(weightingVector) vectors from tempTable group by movie_id") \
            .rdd.map(avg).toDF(["movie_id", "cate_id", "movieVector"])

        # 对计算出的”movieVector“列进行处理，该列为Vector类型，不能直接存入HIVE，HIVE不支持该数据类型
        def toArray(row):
            return row.movie_id, row.cate_id, [float(i) for i in row.movieVector.toArray()]

        movieVector = movieVector.rdd.map(toArray).toDF(['movie_id', 'cate_id', 'movieVector'])

        del topic_weight
        del vectors
        del _movie_profile
        del movieKeywordVectors
        import gc
        gc.collect()

        return movieVector

    def get_user_vector(self, update=None):

        if update:
            user_profile = update.where('cate_id = {}'.format(self.channel_id))
        # 全量的电影画像权重，全量转vector
        else:
            user_profile = self.spark.sql(
                "select * from {}.user_profile where cate_id={}".format(self.user_portrait_db, self.channel_id))

        vectors = self.load_word2vec()
        # 合并计算（对每个词的向量进行权重加权）得到电影每个词的权重向量
        _user_profile = user_profile.join(vectors, vectors.word == user_profile.topic, 'inner')
        # rdd.repartition(100) 可以改变存储分区数量
        userKeywordVectors = _user_profile.rdd.map(lambda row: (row.user_id, row.cate_id, row.topic,
                                                                row.weight * row.vector)).toDF(
            ['user_id', 'cate_id', 'topic', 'weightingVector'])

        # 计算得到用户的平均词向量即用户的向量
        def avg(row):
            x = 0
            for v in row.vectors:
                x += v
            #  将平均向量作为movie的向量
            return row.user_id, row.cate_id, x / len(row.vectors)

        userKeywordVectors.registerTempTable("tempTable")
        userVector = self.spark.sql(
            "select user_id, min(cate_id) cate_id, collect_set(weightingVector) vectors from tempTable group by user_id") \
            .rdd.map(avg).toDF(["user_id", "cate_id", "userVector"])

        # 对计算出的”movieVector“列进行处理，该列为Vector类型，不能直接存入HIVE，HIVE不支持该数据类型
        def toArray(row):
            return row.user_id, row.cate_id, [float(i) for i in row.userVector.toArray()]

        userVector = userVector.rdd.map(toArray).toDF(['user_id', 'cate_id', 'userVector'])

        del user_profile
        del vectors
        del _user_profile
        del userKeywordVectors
        import gc
        gc.collect()

        return userVector


if __name__ == '__main__':
    pass
