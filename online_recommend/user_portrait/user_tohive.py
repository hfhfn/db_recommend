from user_portrait.merge_action import get_merge_action
from user_portrait.pre_user import merge_user_data, get_pre_table, pre_user_data
from user_portrait.user_profile import get_action_topic_sort, get_action_weight, get_action_topic_weight, \
    get_user_profile, normal_action_weight
from utils.default import user_pre_db, user_portrait_db, u_spark, uf_topK
from utils.save_tohive import RetToHive


class SaveUserProfile(object):
    pre_db = user_pre_db
    portrait_db = user_portrait_db
    spark_app = u_spark
    topK = uf_topK


    def save_pre_userdata(self, table_type, table_num, start_num=0):
        """
        保存用户点击数据
        3873, 15328, 69706, 48092, 54080, 73413, 92161, 115959, 164695, 119800, 131214, 133817, 156526
        156044, 168976, 209320, 247980, 275296, 271935, 318472, 369976, 394286
        保存用户收藏数据
        50, 357, 1748, 666, 893, 1247, 1307, 1650, 2095, 1544, 1691, 1835, 1940
        2153, 2093, 3477, 3027, 4424, 3232, 5411, 4346, 4410
        保存用户播放数据
        7 -> 2886,  8-> 82663,  9-> 104271,  10-> 139787,  11-> 204070,  12-> 276867,  13-> 346466,  14-> 477115
        15-> 580029,  16-> 733859,  17-> 878207,  18-> 964412,  19-> 1293973,  20-> 1989563,  21-> 2385531
        :return:
        """
        # table_type = 'click'
        # table_type = 'top'
        # table_type = 'play'

        i = start_num
        # tmp = []
        while i < table_num:  # 修改 i 初始值和while循环的结束值来控制循环表的数量
            pre_table = get_pre_table(self.spark_app, table_type, i)
            try:
                ret = pre_user_data(self.spark_app, table_type, pre_table)
                # count = ret.count()
                # if count == 0:
                #     print("=" * 50 + table_type + '第 {} 个表处理后没有数据'.format(i) + "=" * 50)
                #     i += 1
                #     continue
                table = 'user_{}_{}'.format(table_type, i)
                try:
                    RetToHive(self.spark_app, ret, self.pre_db, table)
                except:
                    print("=" * 50 + table_type + '第 {} 个表处理后没有数据'.format(i) + "=" * 50)
            except:
                print("此数据表处理出现错误，直接跳过")
            i += 1


    def save_merge_userdata(self, table_type, table_num, start_num=0):
        """
        保存 点击数据总表     1月10号以前3590949   8356704
        保存 收藏数据总表     1月10号以前49596    114538
        保存 播放数据总表     1月10号以前6192026    16995825
        :return:
        """
        # table_type = 'click'
        # table_type = 'top'
        # table_type = 'play'

        merge_ret = merge_user_data(self.spark_app, table_type, table_num, start_num)
        merge_table = 'merge_{}'.format(table_type)
        RetToHive(self.spark_app, merge_ret, self.pre_db, merge_table)
        import gc
        del merge_ret
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


    def save_action_weight_normal(self):
        """
        保存 归一化用户对电影的行为权重
        :return:
        """
        action_weight_ret = normal_action_weight(self.portrait_db)
        action_weight_table = 'action_weight_normal'
        RetToHive(self.spark_app, action_weight_ret, self.portrait_db, action_weight_table)
        import gc
        del action_weight_ret
        gc.collect()


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
    pass
