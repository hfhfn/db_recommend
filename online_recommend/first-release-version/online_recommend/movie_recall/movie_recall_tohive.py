from utils import RetToHive, m_spark, movie_recall_db, cate_id, channel, k, minDCS, m_topK
from movie_recall import Word2Vector, BkmeansCossim, get_movie_recall, MovieFilterRecall


class SaveMovieRecall(object):
    spark_app = m_spark
    mf_recall = MovieFilterRecall(spark_app)
    database_name = movie_recall_db
    channel = channel
    topK = m_topK
    cate_id, k, minDCS = cate_id, k, minDCS

    def save_movie_vector(self, refit=True):
        # 保存电影向量数据    47157
        wv = Word2Vector(self.spark_app, refit, self.channel)
        movie_vector_ret = wv.get_movie_vector()
        movie_vector_table = 'movie_vector_{}'.format(self.cate_id)
        RetToHive(self.spark_app, movie_vector_ret, self.database_name, movie_vector_table)

    def save_bkmeans_cluster(self, refit=True):
        # 保存bkmeans聚类结果
        """
        k, minDCS= 50, 800
        :return:
        """
        cos = BkmeansCossim(self.spark_app, self.database_name, k=self.k, minDCS=self.minDCS, channel=self.channel)
        bkmeans_ret = cos.get_bkmeans(refit=refit)
        bkmeans_table = 'movie_bkmeans_{}_k{}m{}'.format(self.cate_id, self.k, self.minDCS)
        RetToHive(self.spark_app, bkmeans_ret, self.database_name, bkmeans_table)

    def save_cos_similar(self, start_group=0):
        # 保存余弦相似度结果
        """
        33600,  53500,  31500,  38900,  40300,  41600,  47700,  46800,  34300,  56300,  36400,  79900,  59900,  22400
        26700,  85400,  184400,  111100,  101600,  53100,  55800,  143100,  153400,  6642,  107700,  96600,  97600
        165400,  84900,  78500,  113900,  73500,  9312,  119200,  162000,  436800,  240500,  176700,  103200,  193400
        137700,  171200,  68300,  55800,  44800,  105800,  111200,  29400,  155600,  30400
        :return:
        """
        for group in range(start_group, self.k):    # group 默认从0开始
            cos = BkmeansCossim(self.spark_app, self.database_name, recall_topK=self.topK, k=self.k,
                                minDCS=self.minDCS, channel=self.channel, group=group)
            cos_sim_ret = cos.get_cos_sim()
            cos_sim_table = 'movie_cos_sim_{}_k{}m{}_{}'.format(self.cate_id, self.k, self.minDCS, group)
            print('={}='.format(group) * 20)
            RetToHive(self.spark_app, cos_sim_ret, self.database_name, cos_sim_table)

    def save_movie_recall(self):
        # 合并 分组召回topk的结果    4713754
        movie_recall_ret = get_movie_recall(self.spark_app, self.cate_id, self.k, self.minDCS)
        movie_recall_table = 'movie_recall_{}_k{}m{}'.format(self.cate_id, self.k, self.minDCS)
        RetToHive(self.spark_app, movie_recall_ret, self.database_name, movie_recall_table)

    def save_movie_filter_version_recall(self):
        # 保存过滤花絮，多版本召回结果    4353129
        movie_recall_ret = self.mf_recall.get_filter_version_recall()
        movie_recall_table = 'movie_filter_version_recall_{}'.format(self.cate_id)
        RetToHive(self.spark_app, movie_recall_ret, self.database_name, movie_recall_table)

    def save_movie_filter_hot_score_year(self):
        # 保存热度、评分、发行年代加权的召回结果    4013596
        movie_recall_ret = self.mf_recall.get_filter_hot_score_year()
        movie_recall_table = 'movie_recall_factor_{}'.format(self.cate_id)
        RetToHive(self.spark_app, movie_recall_ret, self.database_name, movie_recall_table)

    def save_movie_latest_recall(self):
        # 保存基于电影的最终召回结果
        movie_recall_ret = self.mf_recall.get_movie_latest_recall()
        movie_recall_table = 'movie_recall_{}_{}'.format(self.cate_id, self.topK)
        RetToHive(self.spark_app, movie_recall_ret, self.database_name, movie_recall_table)


if __name__ == '__main__':
    SaveMovieRecall().save_movie_vector()
    pass