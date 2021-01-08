from user_portrait import merge_user_data, get_user_play, get_user_collect
from utils import RetToHive, user_pre_db, user_portrait_db, u_spark, uf_topK
from user_portrait import get_merge_action, get_action_topic_sort, get_action_weight, get_action_topic_weight, \
    get_user_profile


class SaveUserProfile(object):
    pre_db = user_pre_db
    portrait_db = user_portrait_db
    spark_app = u_spark
    topK = uf_topK

    def save_user_collect(self):
        """
        :return:
        """
        collect_ret = get_user_collect(self.spark_app)
        collect_table = 'user_collect'
        RetToHive(self.spark_app, collect_ret, self.pre_db, collect_table)
        import gc
        del collect_ret
        gc.collect()

    def save_user_play(self):  # 25504909
        """
        :return:
        """
        play_ret = get_user_play(self.spark_app)
        play_table = 'user_play'
        RetToHive(self.spark_app, play_ret, self.pre_db, play_table)
        import gc
        del play_ret
        gc.collect()

    def save_merge_action(self, start_time=0):
        """
        保存 所有行为数据表     25467028
        :return:
        """
        merge_action_ret = get_merge_action(self.pre_db, start_time)
        merge_action_table = 'merge_action'
        RetToHive(self.spark_app, merge_action_ret, self.pre_db, merge_action_table)
        import gc
        del merge_action_ret
        gc.collect()

    def save_action_weight(self):
        """
        保存 用户对电影的行为权重     3397006
        :return:
        """
        action_weight_ret = get_action_weight(self.pre_db)
        action_weight_table = 'action_weight'
        RetToHive(self.spark_app, action_weight_ret, self.portrait_db, action_weight_table)
        import gc
        del action_weight_ret
        gc.collect()

    # def save_action_weight_normal(self):
    #     """
    #     保存 归一化用户对电影的行为权重
    #     :return:
    #     """
    #     action_weight_ret = normal_action_weight(self.portrait_db)
    #     action_weight_table = 'action_weight_normal'
    #     RetToHive(self.spark_app, action_weight_ret, self.portrait_db, action_weight_table)
    #     import gc
    #     del action_weight_ret
    #     gc.collect()

    def save_action_topic_weight(self):
        """
        保存 行为+主题词+权重数据表  79722719   统计有用户行为的电影数量： 旧数据（18501）
        :return:
        """
        action_topic_ret = get_action_topic_weight(self.portrait_db)
        action_topic_table = 'action_topic_weight'
        RetToHive(self.spark_app, action_topic_ret, self.portrait_db, action_topic_table)
        import gc
        del action_topic_ret
        gc.collect()

    def save_action_topic_sort(self):
        """
        保存 行为+主题词权重排序数据表  58960371
        用户电影类别画像词：旧数据 5640599  统计有用户行为的电影数量： 旧数据 18501
        用户平均画像词数量： 旧数据 5640599/18501 = 305
        看过电影类别的用户数量： 旧数据 44206
        :return:
        """
        action_topic_sort_ret = get_action_topic_sort(self.portrait_db)
        action_topic_sort_table = 'action_topic_sort'
        RetToHive(self.spark_app, action_topic_sort_ret, self.portrait_db, action_topic_sort_table)
        import gc
        del action_topic_sort_ret
        gc.collect()

    def save_user_profile(self):
        """
        用户画像 topK:    11361313
        :return:
        """
        user_profile_ret = get_user_profile(self.portrait_db, self.topK)
        user_profile_table = 'user_profile'
        RetToHive(self.spark_app, user_profile_ret, self.portrait_db, user_profile_table)
        import gc
        del user_profile_ret
        gc.collect()


if __name__ == '__main__':
    # save_user = SaveUserProfile()
    # save_user.save_user_collect()
    # save_user.save_user_play()

    pass
