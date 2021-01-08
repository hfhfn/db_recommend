from pyspark.ml.clustering import BisectingKMeans, BisectingKMeansModel
from utils import movie_model_path, Word2Vector, m_spark, movie_recall_db, m_topK
from utils import MovieDataApp


class LshSimilar(object):

    def __init__(self, spark, database, refit=True, recall_topK=100, channel_id=1969):
        """
        :param recall_num:  int 召回相似电影数量
        """
        self.refit = refit
        self.spark = spark
        self.spark.sql("use {}".format(database))
        self.recall_num = recall_topK
        self.channel_id = channel_id
        self.movieVector = self.load_movie_vector()


    def load_movie_vector(self):
        """
        读取movieVector并转换格式
        :return: 【dataframe】
        """
        movieVector = self.spark.sql("select * from movie_vector_{}".format(self.channel_id))
        # hive中保存的是array类型， 需要转换为vector类型
        from pyspark.ml.linalg import Vectors
        def array_to_vector(row):
            return row.movie_id, row.cate_id, Vectors.dense(row.movieVector)
        movieVector = movieVector.rdd.map(array_to_vector).toDF(['movie_id', 'cate_id', 'movieVector'])
        # movieVector.show()
        return movieVector


    def fit_lsh(self, movieVector):
        """
        训练并保存 lsh 模型
        :return:
        """
        from pyspark.ml.feature import BucketedRandomProjectionLSH
        # 默认4，10，官方推荐使用大小
        brp = BucketedRandomProjectionLSH(inputCol='movieVector', outputCol='hashes', numHashTables=4.0,
                                          bucketLength=10.0)
        brp_model = brp.fit(movieVector)
        # fit_model = brp_model.transform(movieVector).show()
        # fit_model = brp_model.transform(movieVector)
        # print(fit_model.count())
        brp_model.write().overwrite().save(movie_model_path + "channel_%d.lsh" % self.channel_id)

    def get_lsh_similar(self):
        """
        计算 lsh 相似度结果
        :return: 【dataframe】 datasetA和datasetB字段格式不能存入hive
        """
        from pyspark.ml.feature import BucketedRandomProjectionLSHModel
        movieVector = self.movieVector
        # print(movieVector.count())
        if self.refit:
            self.fit_lsh(movieVector)
        brp_model = BucketedRandomProjectionLSHModel.load(movie_model_path + "channel_%d.lsh" % self.channel_id)
        # threshold 为距离最大阈值， 返回值小于2的相似结果
        similar = brp_model.approxSimilarityJoin(movieVector, movieVector, threshold=2, distCol='EucDistance')   # EuclideanDistance

        import pyspark.sql.functions as fn
        from pyspark.sql import Window
        similar = similar.selectExpr("datasetA.movie_id as movie_id","datasetB.movie_id as movie_id2",
                                     "EucDistance as distance").filter("movie_id <> movie_id2")\
                        .withColumn("sort_num", fn.row_number().over(Window.partitionBy("movie_id")
                        .orderBy(similar["distance"].asc()))).where('sort_num <= {}'.format(m_topK))

        return similar




if __name__ == '__main__':
    lsh_similar = LshSimilar(m_spark, movie_recall_db, recall_topK=m_topK, channel_id=1969)
    movie_recall_ret = lsh_similar.get_lsh_similar()
    movie_recall_ret.show()
    pass

