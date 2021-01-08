from utils import RetToHive, m_spark, movie_recall_db, m_topK, update_user_db, FilterRecall, Word2Vector, \
                    LatestFilterRecall
from movie_recall import LshSimilar


class SaveMovieRecall(object):
    spark_app = m_spark
    database_name = movie_recall_db
    update_user_db = update_user_db
    topK = m_topK

    def __init__(self, cate_id):
        self.cate_id = cate_id
        self.mf_recall = LatestFilterRecall(self.spark_app, cate_id, self.database_name)
        self.f_recall = FilterRecall(self.spark_app, cate_id, self.database_name)

    def save_movie_vector(self, refit=True):
        # 保存电影向量数据    47157
        wv = Word2Vector(self.spark_app, refit, self.cate_id)
        movie_vector_ret = wv.get_movie_vector()
        movie_vector_table = 'movie_vector_{}'.format(self.cate_id)
        RetToHive(self.spark_app, movie_vector_ret, self.database_name, movie_vector_table)
        import gc
        del movie_vector_ret
        del wv
        gc.collect()

    def save_movie_recall(self):
        # 保存电影相似度召回 7683103
        lsh_similar = LshSimilar(self.spark_app, self.database_name, recall_topK=self.topK, channel_id=self.cate_id)
        movie_recall_ret = lsh_similar.get_lsh_similar()
        movie_recall_table = 'movie_recall_{}'.format(self.cate_id)
        RetToHive(self.spark_app, movie_recall_ret, self.database_name, movie_recall_table)


    def save_movie_filter_version_recall(self):
        # 保存过滤花絮，多版本召回结果    4353129
        movie_recall_ret = self.f_recall.get_filter_version_recall(filter='movie')
        movie_recall_table = 'movie_filter_version_recall_{}'.format(self.cate_id)
        RetToHive(self.spark_app, movie_recall_ret, self.database_name, movie_recall_table)

    def save_movie_filter_hot_score_year(self):
        # 保存热度、评分、发行年代加权的召回结果    4013596
        movie_recall_ret = self.f_recall.get_filter_hot_score_year(filter='movie')  # 默认就是 'movie'
        movie_recall_table = 'movie_recall_factor_{}'.format(self.cate_id)
        RetToHive(self.spark_app, movie_recall_ret, self.database_name, movie_recall_table)

    def save_movie_latest_recall(self):
        # 保存基于电影的最终召回结果
        movie_recall_ret = self.mf_recall.get_movie_latest_recall()
        movie_recall_table = 'movie_recall_{}_{}'.format(self.cate_id, self.topK)
        RetToHive(self.spark_app, movie_recall_ret, self.database_name, movie_recall_table)


if __name__ == '__main__':
    save_movie = SaveMovieRecall(cate_id=1969)
    # save_movie.save_movie_vector()
    save_movie.save_movie_recall()
    pass