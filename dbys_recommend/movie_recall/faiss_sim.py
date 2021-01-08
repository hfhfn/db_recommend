import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE_DIR))


import gc
# import pyspark.sql.functions as F
# from functools import partial
# from movie_portrait import rank_map, get_topK_weight, get_cv_idf_model
from utils import mf_topK, dim, k, m_topK, m_spark, LatestFilterRecall,\
    movie_recall_db, RetToHive, MovieCutWords, get_filter_data, FilterRecall
import logging


logger = logging.getLogger('offline')


class FaissSimilar(object):
    dim = dim
    k = k
    m_topK = m_topK
    spark = m_spark
    movie_recall_db = movie_recall_db

    def __init__(self, cate_id):
        self.cate_id = cate_id


    def compute_movie_similar(self):
        """
        计算电影的相似度 word2vec
        从计算电影向量开始，分类型处理
        :return:
        """

        # 直接读取全量的 movie_vector 进行计算
        self.spark.sql("use {}".format(self.movie_recall_db))
        movie_vector = self.spark.sql("select * from movie_vector_{}".format(self.cate_id))

        import numpy as np
        # update_movieVector_list = movie_vector.toPandas().values[:, 2].tolist()
        # update_movieVector_id = movie_vector.select('movie_id').toPandas()
        # update_movieVector_array = np.asarray(update_movieVector_list).astype('float32')

        movie_vector_id = movie_vector.select('movie_id').toPandas()
        movie_vector_list = movie_vector.toPandas().values[:, 2].tolist()
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
        # recall_df.write.insertInto('movie_similar_{}'.format(self.cate_id), overwrite=True)
        return recall_df



if __name__ == '__main__':
    ua = FaissSimilar(1969)
    movie_similar = ua.compute_movie_similar()
    pass
