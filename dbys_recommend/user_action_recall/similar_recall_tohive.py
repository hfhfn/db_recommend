from utils import RetToHive, u_spark, user_recall_db, u_topK, user_portrait_db, online_db, FilterRecall
from user_action_recall import UserSimilarRecall


class SaveUserSimilarRecall(object):
    spark_app = u_spark
    recall_db = user_recall_db
    portrait_db = user_portrait_db
    online_db = online_db
    topK = u_topK

    def __init__(self, cate_id):
        self.cate_id = cate_id
        self.user_recall = UserSimilarRecall(self.portrait_db, self.recall_db, self.spark_app, cate_id)
        self.f_recall = FilterRecall(self.spark_app, cate_id, self.recall_db)

    def save_user_similar_recall(self):
        # 保存用户行为电影相似度召回结果
        recall_ret = self.user_recall.get_user_similar_recall()
        recall_table = 'user_similar_recall_{}'.format(self.cate_id)
        RetToHive(self.spark_app, recall_ret, self.recall_db, recall_table)
        import gc
        del recall_ret
        gc.collect()

    def save_filter_same_recall(self):
        # 综合用户行为电影相似度召回相同的结果
        recall_ret = self.user_recall.get_filter_same_recall()
        recall_table = 'user_similar_filter_same_recall_{}'.format(self.cate_id)
        RetToHive(self.spark_app, recall_ret, self.recall_db, recall_table)
        import gc
        del recall_ret
        gc.collect()

    def save_filter_history_recall(self):
        # 过滤用户历史数据
        recall_ret = self.f_recall.get_filter_history_recall(recall='action')
        recall_table = 'user_similar_filter_history_recall_{}'.format(self.cate_id)
        RetToHive(self.spark_app, recall_ret, self.recall_db, recall_table)
        import gc
        del recall_ret
        gc.collect()

    def save_user_similar_latest_recall(self):
        # 保存topK个召回结果
        recall_ret = self.user_recall.get_user_similar_latest_recall()
        recall_table = 'user_similar_recall_{}_{}'.format(self.cate_id, self.topK)
        RetToHive(self.spark_app, recall_ret, self.recall_db, recall_table)
        # 精简上线数据
        # recall_ret = self.spark_app.sql("select * from update_user.user_similar_recall_1969_100")  # 零时用
        recall_ret = recall_ret.select('user_id', 'movie_id', 'title', 'sort_num', 'timestamp')
        recall_ret.write.insertInto("{}.{}".format(self.online_db, recall_table), overwrite=True)
        # RetToHive(self.spark_app, recall_ret, self.online_db, recall_table)
        import gc
        del recall_ret
        gc.collect()



if __name__ == '__main__':
    usr = SaveUserSimilarRecall(1969)
    usr.save_user_similar_latest_recall()

    pass
