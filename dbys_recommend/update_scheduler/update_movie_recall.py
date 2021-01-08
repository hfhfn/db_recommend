import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE_DIR))


import gc
import pyspark.sql.functions as F
from functools import partial
from movie_portrait import rank_map, get_topK_weight, get_cv_idf_model
from utils import mf_topK, dim, k, m_topK, cv_path, idf_path, um_spark, Word2Vector, LatestFilterRecall,\
    movie_recall_db, RetToHive, MovieCutWords, update_user_db, u_topK, get_filter_data, FilterRecall, user_recall_db
import logging


logger = logging.getLogger('offline')


class UpdateMovie(object):
    dim = dim
    k = k
    m_topK = m_topK
    u_topK = u_topK
    cv_path = cv_path
    idf_path = idf_path
    spark = um_spark
    movie_recall_db = movie_recall_db
    user_recall_db = user_recall_db
    update_user_db = update_user_db

    def __init__(self, channel, cate_id):
        self.channel = channel
        self.cate_id = cate_id

    def get_cv_model(self):
        # 词语与词频统计
        from pyspark.ml.feature import CountVectorizerModel
        cv_model = CountVectorizerModel.load(self.cv_path)
        return cv_model

    def get_idf_model(self):
        from pyspark.ml.feature import IDFModel
        idf_model = IDFModel.load(self.idf_path)
        return idf_model

    @staticmethod
    def compute_keywords_tfidf(words_df, cv_model, idf_model):
        """保存tfidf值
        :param spark:
        :param words_df:
        :return:
        """
        cv_result = cv_model.transform(words_df)
        tfidf_result = idf_model.transform(cv_result)
        # print("transform compelete")
        keywords_list_with_idf = list(zip(cv_model.vocabulary, idf_model.idf.toArray()))

        # 整合TFIDF的结果
        def func(partition):
            for row in partition:
                indices_li = row.features.indices
                values_li = row.features

                for index in indices_li:
                    index = int(index)
                    word, idf = keywords_list_with_idf[index]
                    tfidf = values_li[index]
                    yield row.movie_id, row.cate_id, row.title, word, index, round(float(idf), 4), round(float(tfidf), 4)

        _keywordsByTFIDF = tfidf_result.rdd.mapPartitions(func).toDF(
            ["movie_id", "cate_id", "title", "keyword", "index", "idf", "tfidf"])
        # _keywordsByTFIDF.show()

        return _keywordsByTFIDF

    def merge_movie_data(self, full=False):
        """
        合并业务中增量更新的电影数据
        :return:
        """
        # 获取电影相关数据, 指定过去一个月到现在的更新数据
        # 如：上个月26日：1：00~现在1：00， 左闭右开
        self.spark.sql("use movie")
        import time
        import datetime
        today = datetime.date.today()
        _yesterday = today - datetime.timedelta(days=1)
        day_timestamp = int(time.mktime(time.strptime(str(today), '%Y-%m-%d')))  # 零点时间戳
        yesterday = int(time.mktime(time.strptime(str(_yesterday), '%Y-%m-%d')))  # 零点时间戳
        if full:
            start = 0
        else:
            start = yesterday
        end = day_timestamp
        # print(start, end)
        # _yester = datetime.today().replace(minute=0, second=0, microsecond=0)
        # start = datetime.strftime(_yester + timedelta(days=0, hours=-1, minutes=0), "%Y-%m-%d %H:%M:%S")
        # end = datetime.strftime(_yester, "%Y-%m-%d %H:%M:%S")
        update_df = self.spark.sql(
            "select * from db_asset where create_time >= '{}' and create_time < '{}'".format(start, end))
        # print(update_df.count())
        update_df = get_filter_data(update_df)
        # 利用concat_ws方法，将多列数据合并为一个长文本内容（频道，标题以及内容合并）
        summary_df = update_df.select("id", "cid", "title", "create_time", \
                                      F.concat_ws(
                                          ",",
                                          update_df.title, \
                                          update_df.cate, \
                                          update_df.actor, \
                                          update_df.director, \
                                          update_df.area, \
                                          update_df.desc, \
                                          ).alias("summary")
                                      )
        del update_df
        gc.collect()
        self.spark.sql("use movie_portrait")
        if full:
            """
            insertInto 标记一下
            """
            summary_df.write.insertInto("movie_feature", overwrite=True)
        else:
            # 查看 movie_feature 是否已经写入当前数据， 如果已写入则不再写入，否则正常写入当前数据
            df = self.spark.sql("select id from movie_feature where create_time >= '{}'".format(start))
            if df.rdd.count():
                pass
            else:
                """
                insertInto 标记一下
                """
                summary_df.write.insertInto("movie_feature", overwrite=False)

        return summary_df

    def generate_movie_label(self, summary_df, full=False):
        """
        生成电影标签  tfidf, textrank
        :param sentence_df: 增量的电影内容
        :return:
        """
        # 进行分词
        segmentation = MovieCutWords().segmentation
        words_df = summary_df.rdd.mapPartitions(segmentation).toDF(
                                ["movie_id", "cate_id", "title", "words", "create_time"])
        import time
        import datetime
        today = datetime.date.today()
        _yesterday = today - datetime.timedelta(days=1)
        yesterday = int(time.mktime(time.strptime(str(_yesterday), '%Y-%m-%d')))  # 零点时间戳
        if not full:
            # 查看 cut_words 是否已经写入当前数据， 如果已写入则 跳过写入，否则正常写入当前数据
            df = self.spark.sql("select movie_id from cut_words where create_time >= '{}'".format(yesterday))
            if df.rdd.count():
                pass
            else:
                """
                insertInto 标记一下
                """
                words_df.write.insertInto("cut_words", overwrite=full)
        else:
            """
            insertInto 标记一下
            """
            words_df.write.insertInto("cut_words", overwrite=full)
        # 重新训练cv和idf模型， 基于全量的 cut_words
        get_cv_idf_model()
        cv_model = self.get_cv_model()
        idf_model = self.get_idf_model()

        if full:
            # 用于计算全量的tfidf
            words_df = self.spark.sql("select * from cut_words")
            # 用于计算全量的textrank
            summary_df = self.spark.sql("select * from movie_feature")

        # 1、保存所有的词的idf的值，利用idf中的词的标签索引
        # 工具与业务隔离
        keywordsByTFIDF = UpdateMovie.compute_keywords_tfidf(words_df, cv_model, idf_model)
        """
        insertInto 标记一下
        """
        # overwrite 传参 full， 全量是为True， 增量为False
        keywordsByTFIDF.write.insertInto("tfidf", overwrite=full)

        del cv_model
        del idf_model
        del words_df
        gc.collect()

        # 计算textrank
        textrank = partial(rank_map, topK=None)
        textrank_keywords_df = summary_df.rdd.mapPartitions(textrank).toDF(
            ["movie_id", "cate_id", "title", "tag", "textrank"])
        """
        insertInto 标记一下
        """
        # overwrite 传参 full， 全量是为True， 增量为False
        textrank_keywords_df.write.insertInto("textrank", overwrite=full)

        return textrank_keywords_df, keywordsByTFIDF

    def get_movie_profile(self, textrank, tfidf, full=False):
        """
        电影画像主题词建立
        :param idf: 所有词的idf值
        :param textrank: 每个电影的textrank值
        :return: 返回增量画像
        """
        tfidf = tfidf.drop('cate_id').withColumnRenamed('movie_id', 'mid')
        result = textrank.join(tfidf, (textrank.tag == tfidf.keyword) & (textrank.movie_id == tfidf.mid))

        # 1、关键词（词，权重）
        # 计算关键词权重
        idf_rank_weights = result.withColumn("weights", result.textrank * result.idf).select(
            ["movie_id", "cate_id", "keyword", "weights"])
        tfidf_rank_weights = result.withColumn('weights', (result.textrank + result.tfidf) / 2).select(
            ['movie_id', 'cate_id', 'keyword', 'weights'])
        topK_idf_textrank = get_topK_weight(idf_rank_weights, mf_topK)
        topK_tfidf_textrank = get_topK_weight(tfidf_rank_weights, mf_topK).drop('weight')

        # 2、主题词
        import time
        import datetime
        today = datetime.date.today()
        _yesterday = today - datetime.timedelta(days=1)
        yesterday = int(time.mktime(time.strptime(str(_yesterday), '%Y-%m-%d')))  # 零点时间戳
        # 将tfidf_textrank和idf_textrank共现的词作为主题词
        topic_weight = topK_idf_textrank.join(topK_tfidf_textrank, on=['movie_id', 'cate_id', 'keyword']) \
            .withColumnRenamed('keyword', 'topic').withColumn('timestamp', F.lit(yesterday))

        # 3、将主题词权重增量更新
        if not full:
            # 查看 topic_weights 是否已经写入当前数据， 如果已写入则 跳过写入，否则正常写入当前数据
            df = self.spark.sql("select movie_id from topic_weights where timestamp = '{}'".format(yesterday))
            if df.rdd.count():
                pass
            else:
                """
                insertInto 标记一下
                """
                topic_weight.write.insertInto("topic_weights", overwrite=full)
        else:
            """
            insertInto 标记一下
            """
            # overwrite 传参 full， 全量是为True， 增量为False
            topic_weight.write.insertInto("topic_weights", overwrite=full)

        del result
        del idf_rank_weights
        del tfidf_rank_weights
        del topK_idf_textrank
        del topK_tfidf_textrank
        gc.collect()

        return topic_weight

    def compute_movie_similar(self, movieProfile=None, refit=False):
        """
        计算增量电影与历史电影的相似度 word2vec
        从计算电影向量开始，分类型处理
        :return:
        """
        # 增量更新方式得到 movie_vector不合理， 舍弃
        """
        wv = Word2Vector(self.spark, refit, self.channel)
        movie_vector = wv.get_movie_vector(movieProfile)
        # movie_vector.show()    # movie_id, cate_id, movieVector

        # insertInto 标记一下
        self.spark.sql("use movie_recall")
        movie_vector.write.insertInto('movie_vector_{}'.format(self.cate_id), overwrite=False)
        """
        # 直接读取全量的 movie_vector 进行计算
        self.spark.sql("use {}".format(self.movie_recall_db))
        movie_vector = self.spark.sql("select * from movie_vector_{}".format(self.cate_id))

        import numpy as np
        # update_movieVector_list = movie_vector.toPandas().values[:, 2].tolist()
        # update_movieVector_id = movie_vector.select('movie_id').toPandas()
        # update_movieVector_array = np.asarray(update_movieVector_list).astype('float32')

        # 不引用上面的movie_vector_{}, 而是读取它， 是为了得到全量的数据
        # movie_vector_list = self.spark.sql('select * from movie_vector_{}'.format(self.cate_id)).toPandas() \
        #                     .values[:, 2].tolist()

        movie_vector_list = movie_vector.toPandas().values[:, 2].tolist()
        movie_vector_id = movie_vector.select('movie_id').toPandas()
        movie_vector_array = np.asarray(movie_vector_list).astype('float32')

        # del wv
        # del update_movieVector_list
        del movie_vector
        del movie_vector_list
        gc.collect()

        import faiss
        # quantizer = faiss.IndexFlatL2(self.dim)  # L2距离，即欧式距离（越小越好）
        quantizer = faiss.IndexFlatIP(self.dim)  # 点乘，归一化的向量点乘即cosine相似度（越大越好）
        m = 4  # 把原始的向量空间分解为m个低维向量空间的笛卡尔积
        gzip = 8  # 每个向量压缩为8bit
        index = faiss.IndexIVFPQ(quantizer, self.dim, self.k, m, gzip)
        index.nprobe = 5  # 同时查找聚类中心的个数，默认为1个，若nprobe=nlist(self.k)则等同于精确查找
        # print(index.is_trained)
        index.train(movie_vector_array)  # 需要训练
        index.add(movie_vector_array)  # 添加训练时的样本
        # print(index.ntotal)
        # D, I = index.search(update_movieVector_array, self.m_topK)  # 寻找相似向量， I表示相似用户ID矩阵， D表示距离矩阵
        D, I = index.search(movie_vector_array, self.m_topK)  # 寻找相似向量， I表示相似用户ID矩阵， D表示距离矩阵
        # print(D[:5])
        # print(I[-5:])

        import pandas as pd
        df = pd.DataFrame(columns=['_index'])
        df.append(pd.Series([None]), ignore_index=True)
        df['_index'] = I.tolist()
        df2 = pd.DataFrame(columns=['similar'])
        df2.append(pd.Series([None]), ignore_index=True)
        df2['similar'] = D.tolist()

        concat_df = pd.concat([movie_vector_id, df, df2], axis=1)
        sp_df = self.spark.createDataFrame(concat_df)

        # from pyspark.shell import sqlContext
        # sp_df.registerTempTable("tmp_df")
        # sqlContext.cacheTable("tmp_df")
        # 此处也是取全量的 movie_id
        # movie_vector_id = self.spark.sql('select movie_id from movie_vector_{}'.format(self.cate_id)).toPandas()
        # movie_vector_id = movie_vector.select('movie_id').toPandas()

        def map(partition):
            for row in partition:
                for i in range(self.m_topK):
                    # # L2_Distance 代表 向量做L2归一化后的欧氏距离
                    # cos_sim = (2 - row.similar[i]) / 2  # 欧式距离和余弦的转换 cosine = (2 - L2_Distance) / 2
                    movie_id2 = movie_vector_id.iloc[row._index[i], 0]
                    if row.movie_id == movie_id2:
                        continue
                    yield int(row.movie_id), int(movie_id2), row.similar[i]

        # from pyspark import StorageLevel
        # recall_df = sp_df.rdd.persist(StorageLevel.MEMORY_AND_DISK).repartition(1000).mapPartitions(map).toDF(
        #     ['movie_id', 'movie_id2', 'cos_sim'])
        # sp_df = self.spark.sql('select * from tmp_df')
        # recall_df = sp_df.rdd.repartition(1000).mapPartitions(map).toDF(['movie_id', 'movie_id2', 'cos_sim'])
        recall_df = sp_df.rdd.mapPartitions(map).toDF(['movie_id', 'movie_id2', 'cos_sim'])

        # sqlContext.uncacheTable("tmp_df")
        # del movie_vector
        # del update_movieVector_id
        # del update_movieVector_array
        del movie_vector_id
        del movie_vector_array
        del index
        del df
        del df2
        del concat_df
        del sp_df
        del D
        del I
        gc.collect()

        # RetToHive(self.spark, recall_df, self.movie_recall_db, 'movie_similar_{}'.format(self.cate_id))
        recall_df.write.insertInto('movie_similar_{}'.format(self.cate_id), overwrite=True)
        return recall_df



if __name__ == '__main__':
    ua = UpdateMovie('电影', '1969')
    # sentence_df = ua.merge_movie_data()
    # if sentence_df.rdd.count():
    #     rank, idf = ua.generate_movie_label(sentence_df)
    #     movieProfile = ua.get_movie_profile(rank, idf)
    #     movie_similar = ua.compute_movie_similar(movieProfile, refit=False)
    #     movie_recall = ua.get_filter_latest_recall(movie_similar)

    ua.merge_movie_data()
    pass
