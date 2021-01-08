from pyspark.ml.clustering import BisectingKMeans, BisectingKMeansModel
from utils.default import channelInfo
from utils.spark_app import MovieDataApp


class BkmeansCossim(object):

    def __init__(self, spark, database, group=0, recall_topK=50, k=100, minDCS=200, seed=10, channel='电影'):
        """
        :param group:   int 计算第几组聚类相似度
        :param recall_num:  int 召回相似电影数量
        :param k:  int 聚类中心数
        """
        self.spark = spark
        self.spark.sql("use {}".format(database))
        self.movieVector = self.load_movie_vector()
        self.group = group
        self.recall_num = recall_topK
        self.k = k
        self.minDCS = minDCS
        self.seed = seed

        self.channel = channel
        self.channel_id = channelInfo[self.channel]
        self.similar_model_path = "hdfs://hadoop-master:8020/movie/models/similar/channel_%d_%s_k%dm%d.bkmeans" \
                             % (self.channel_id, self.channel, self.k, self.minDCS)

    def load_movie_vector(self):
        """
        读取movieVector并转换格式
        :return: 【dataframe】
        """
        movieVector = self.spark.sql("select * from movie_vector")
        # hive中保存的是array类型， 需要转换为vector类型
        from pyspark.ml.linalg import Vectors
        def array_to_vector(row):
            return row.movie_id, row.cate_id, Vectors.dense(row.movieVector)
        movieVector = movieVector.rdd.map(array_to_vector).toDF(['movie_id', 'cate_id', 'movieVector'])
        # movieVector.show()
        return movieVector

    def fit_bkmeans(self):
        """
        训练并保存bkmeans模型
        :return:
        """
        bkmeans = BisectingKMeans(k=self.k, minDivisibleClusterSize=self.minDCS, featuresCol="movieVector",
                                  predictionCol='group', seed=self.seed)
        bkmeans_model = bkmeans.fit(self.movieVector)
        #  minDivisibleClusterSize=50  =>   k=100  Errors = 135931.2    k=50  150118.78
        #  minDivisibleClusterSize=200  =>   k=100  Errors = 134837.76   k=50  150416.65
        #  minDivisibleClusterSize=400  =>   k=50  149343.97
        #  minDivisibleClusterSize=600  =>   k=50  148595.19
        #  minDivisibleClusterSize=800  =>   k=50  148424.24
        bkmeans_model.write().overwrite().save(self.similar_model_path)
        return bkmeans_model

    def get_bkmeans(self, refit):
        """
        计算bkmeans结果
        :return: 【dataframe】
        """
        if not refit:
            bkmeans_model = BisectingKMeansModel.load(self.similar_model_path)
        else:
            bkmeans_model = self.fit_bkmeans()
        bkmeans_group = bkmeans_model.transform(self.movieVector).select("movie_id", "cate_id", "movieVector",
                                                                             "group")

        def toArray(row):
            return row.movie_id, row.cate_id, [float(i) for i in row.movieVector.toArray()], row.group
        bkmeans_group = bkmeans_group.rdd.map(toArray).toDF(['movie_id', 'cate_id', 'movieVector', 'group'])

        # bkmeans_group.collect()
        # bkmeans_group.show()

        # bkmeans_group[0].prediction == bkmeans_group[1].prediction  => True/False

        # Evaluate clustering.
        cost = bkmeans_model.computeCost(self.movieVector)
        print("Within Set Sum of Squared Errors = " + str(cost))

        # # Shows the result.
        # print("Cluster Centers: ")
        # centers = bkmeans_model.clusterCenters()
        # for center in centers:
        #     print(center)
        return bkmeans_group

    def load_movie_bkmeans(self):
        """
        读取movie_bkmeans并转换格式
        :return: 【dataframe】
        """
        movieVector = self.spark.sql("select * from movie_bkmeans_{}_k{}m{}"
                     .format(self.channel_id, self.k, self.minDCS)).filter('group = {}'.format(self.group))
        # hive中保存的是array类型， 需要转换为vector类型
        from pyspark.ml.linalg import Vectors
        def array_to_vector(row):
            return row.movie_id, Vectors.dense(row.movieVector)
        movieVector = movieVector.rdd.map(array_to_vector).toDF(['movie_id', 'movieVector'])
        # movieVector.show()
        return movieVector

    def get_cos_sim(self):
        """
        计算余弦相似度
        :return: 【dataframe】
        """
        # from pyspark import SparkConf, SparkContext
        # SparkContext()
        # SparkConf().set("spark.storage.memoryFraction","0.6")
        import numpy as np
        # import gc
        movieVector = self.load_movie_bkmeans()
        temp_df = movieVector.withColumnRenamed("movie_id", "movie_id2").withColumnRenamed("movieVector", "movieVector2")
        # print(temp_df.count())
        movie_vector_join = movieVector.join(temp_df, movieVector.movie_id != temp_df.movie_id2, how="outer")
        # print(movie_vector_join.count())
        def cal_cos_similar(row):
            vector1 = row.movieVector
            vector2 = row.movieVector2
            # 余弦相似度计算公式
            sim = np.dot(vector1, vector2) / (np.linalg.norm(vector1) * (np.linalg.norm(vector2)))
            return row.movie_id, row.movie_id2, float('%.4f' % sim)
        # persist 配置memory_and_disk，内存不足保存到硬盘，作用有限
        from pyspark import StorageLevel
        ret = movie_vector_join.rdd.persist(storageLevel=StorageLevel.MEMORY_AND_DISK).map(cal_cos_similar)\
                                        .toDF(['movie_id', 'movie_id2', 'cos_sim'])
        import pyspark.sql.functions as fn
        from pyspark.sql import Window
        # 对结果进行分组排序，取每组前一百个最相似的
        ret = ret.withColumn("sort_num", fn.row_number().over(Window.partitionBy("movie_id")
                        .orderBy(ret["cos_sim"].desc()))).where("sort_num <= {}".format(self.recall_num))
        # gc.collect()
        # print(ret.count())
        # ret.groupby('movie_id').count().show()
        return ret


if __name__ == '__main__':
    pass







# def fit_lsh():
#     """
#     训练并保存 lsh 模型
#     :return:
#     """
#     from pyspark.ml.feature import BucketedRandomProjectionLSH
#     movieVector = load_movie_vector()
#     # 默认4，10，官方推荐使用大小
#     brp = BucketedRandomProjectionLSH(inputCol='movieVector', outputCol='hashes', numHashTables=4.0,
#                                       bucketLength=10.0)
#     brp_model = brp.fit(movieVector)
#     # fit_model = brp_model.transform(movieVector).show()
#     # fit_model = brp_model.transform(movieVector)
#     # print(fit_model.count())
#     brp_model.write().overwrite().save(similar_model_path + "channel_%d_%s.lsh" % (channel_id, channel))
#
# def get_lsh_similar():
#     """
#     计算 lsh 相似度结果
#     :return: 【dataframe】 datasetA和datasetB字段格式不能存入hive
#     """
#     from pyspark.ml.feature import BucketedRandomProjectionLSHModel
#     movieVector = load_movie_vector()
#     # print(movieVector.count())
#
#     brp_model = BucketedRandomProjectionLSHModel.load(similar_model_path + "channel_%d_%s.lsh" % (channel_id, channel))
#     # threshold 为距离最大阈值， 返回值小于2的相似结果
#     similar = brp_model.approxSimilarityJoin(movieVector, movieVector, threshold=10, distCol='EucDistance')   # EuclideanDistance
#     # similar.sort(['EucDistance'], ascending=False).filter("datasetA != datasetB").show()
#     print(similar.count())
#     import pyspark.sql.functions as fn
#     from pyspark.sql import Window
#     similar = similar.withColumn("row_number", fn.row_number().over(Window.partitionBy("datasetB.movie_id")
#                     .orderBy(similar["EucDistance"].asc())))
#                     # .orderBy(similar["EucDistance"].asc())))
#     similar.groupby('datasetB.movie_id').count().show()
#     # print(similar.count())
#     # similar = similar.where("datasetA.movie_id != datasetB.movie_id")
#     # print(similar.count())
#     # .where("datasetA.movie_id != datasetB.movie_id and row_number <= 100")
#     # print(similar.count())
#     return similar