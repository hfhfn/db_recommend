from action_profile_recall import UserRecall, UserFilterRecall, SaveUserRecall
from action_similar_recall import UserSimilarRecall, SaveUserSimilarRecall
from update_scheduler import UpdateMovie
from user_portrait import SaveUserProfile
from utils import RetToHive, um_spark, update_movie_db, cate_id, uu_spark, update_user_db, pre_topK, u_topK, \
    user_recall_db, factor_db, user_pre_db


class SaveUpdateMovieRecall(object):
    spark_app = um_spark
    database_name = update_movie_db
    cate_id = cate_id
    ua = UpdateMovie()

    def save_update_movie_profile(self):
        sentence_df = self.ua.merge_movie_data()
        if sentence_df.rdd.count():
            rank, idf = self.ua.generate_movie_label(sentence_df)
            movieProfile = self.ua.get_movie_profile(rank, idf)

            # 保存更新的电影召回数据
            movie_profile_table = 'update_movie_profile'
            RetToHive(self.spark_app, movieProfile, self.database_name, movie_profile_table)

    def save_update_movie_similar(self, refit=False):
        movieProfile = self.spark_app.sql(
            'select * from {}.update_movie_profile'.format(self.database_name))
        # 在此方法中更新movie_vector， 更新电影多的时候，最好是基于更新后的全量movie_vector进行相似度计算
        # 即 refit = True 重新训练Word2Vec模型  ==》  一周更新一次
        movie_similar = self.ua.compute_movie_similar(movieProfile, refit=refit)

        # 保存更新的电影召回数据
        movie_recall_table = 'update_movie_similar_{}'.format(self.cate_id)
        RetToHive(self.spark_app, movie_similar, self.database_name, movie_recall_table)

    def save_update_movie_recall(self):
        # movie_similar = self.spark_app.sql(
        #     'select * from {}.update_movie_similar_{}'.format(self.database_name, self.cate_id))
        movie_similar = self.spark_app.sql(
            'select * from {}.movie_similar_{}'.format('movie_recall', self.cate_id))
        movie_recall = self.ua.get_filter_latest_recall(movie_similar)

        # 保存更新的电影召回数据
        movie_recall_table = 'update_movie_recall_{}'.format(self.cate_id)
        RetToHive(self.spark_app, movie_recall, self.database_name, movie_recall_table)


class SaveUpdateUserProfile(SaveUserProfile):
    pre_db = update_user_db
    portrait_db = update_user_db
    spark_app = uu_spark
    user_pre_db = user_pre_db

    def save_merge_action(self, start_time=0):
        """
        保存 所有行为数据表     25467028
        :return:
        """
        merge_action_ret = self.spark_app.sql("select * from {}.merge_action".format(self.user_pre_db)).where(
            'datetime > {}'.format(start_time))
        merge_action_table = 'merge_action'
        RetToHive(self.spark_app, merge_action_ret, self.pre_db, merge_action_table)


class SaveUpdateUserProfileRecall(SaveUserRecall):
    spark_app = uu_spark
    user_recall = UserRecall(update_user_db, update_user_db, spark_app)
    user_filter_recall = UserFilterRecall(update_user_db, update_user_db, spark_app)
    database_name = update_user_db
    pre_topK = pre_topK
    topK = u_topK
    cate_id = cate_id


class SaveUpdateUserSimilarRecall(SaveUserSimilarRecall):
    spark_app = uu_spark
    user_recall = UserSimilarRecall(update_user_db, update_user_db, spark_app)
    database_name = update_user_db
    cate_id = cate_id
    topK = u_topK
