from collections import defaultdict
from tqdm import tqdm
from utils import u_spark, user_portrait_db, RetToHive, user_recall_db, u_topK
from neighbourhood_CF import item_cf_interface, item_cos_cf_interface, user_cf_interface, UserSimilarity, \
        pre_sim_user, merge_dict
from pyspark.sql import functions as F
import numpy as np


class Eval(object):

    def __init__(self, cate_id):
        self.cate_id = cate_id

    def split_data(self, inverted=False, refit=False):
        """
        分割数据集
        :return:
        """
        if refit:
            action_weight = u_spark.sql('select * from {}.action_weight'.format(user_portrait_db))

            trainDF, testDF = action_weight.randomSplit([0.8, 0.2], seed=20)

            train = trainDF.groupBy('user_id', 'cate_id') \
                .agg(F.collect_list('movie_id').alias('movie_li'), F.collect_list('weight').alias('weight_li'))
            # trainDF.show()
            test = testDF.groupBy('user_id', 'cate_id') \
                .agg(F.collect_list('movie_id').alias('movie_li'), F.collect_list('weight').alias('weight_li'))

            # testDF.show()
            def map(row):
                movie_dict = dict(zip(row.movie_li, row.weight_li))
                return row.user_id, row.cate_id, movie_dict

            train = train.rdd.map(map).toDF(['user_id', 'cate_id', 'movie_dict'])
            test = test.rdd.map(map).toDF(['user_id', 'cate_id', 'movie_dict'])

            RetToHive(u_spark, train, user_portrait_db, 'cf_trainDF')
            RetToHive(u_spark, test, user_portrait_db, 'cf_testDF')
            train = train.where('cate_id = {}'.format(self.cate_id)).toPandas()  # .set_index('user_id')
            test = test.where('cate_id = {}'.format(self.cate_id)).toPandas().set_index('user_id')

            if inverted:
                inverted_train = trainDF.groupBy('movie_id', 'cate_id') \
                .agg(F.collect_list('user_id').alias('user_li'), F.collect_list('weight').alias('weight_li'))

                def inverted_map(row):
                    user_dict = dict(zip(row.user_li, row.weight_li))
                    return row.movie_id, row.cate_id, user_dict
                inverted_train = inverted_train.rdd.map(inverted_map).toDF(['movie_id', 'cate_id', 'user_dict'])
                RetToHive(u_spark, inverted_train, user_portrait_db, 'inverted_cf_trainDF')
                inverted_train = inverted_train.where('cate_id = {}'.format(self.cate_id)).toPandas()

                return train, test, inverted_train
        else:
            train = u_spark.sql('select * from {}.cf_trainDF'.format(user_portrait_db)) \
                .where('cate_id = {}'.format(self.cate_id)).toPandas()  # .set_index('user_id')
            test = u_spark.sql('select * from {}.cf_testDF'.format(user_portrait_db)) \
                .where('cate_id = {}'.format(self.cate_id)).toPandas().set_index('user_id')
            if inverted:
                inverted_train = u_spark.sql('select * from {}.inverted_cf_trainDF'.format(user_portrait_db)) \
                .where('cate_id = {}'.format(self.cate_id)).toPandas()

                return train, test, inverted_train
        return train, test

    def get_recommend(self, train, inverted_train=None, cf_method='itemCF', refit=True):
        """
        获取召回结果
        :param train:
        :param cf_method:  itemCF, cos_itemCF, userCF
        :param refit:
        :return:
        """
        recommendation = None
        if cf_method == 'cos_itemCF':
            recall_table = 'cos_trainCF'
            sim_matrix = 'cos_train_w'
            if refit:
                recommendation = item_cos_cf_interface(train, self.cate_id, recall_table, sim_matrix, refit) \
                    .groupBy('user_id').agg(F.collect_list('movie_id').alias('movie_li')).toPandas()\
                    .set_index('user_id')
            else:
                recommendation = u_spark.sql('select * from {}.{}_recall_{}'
                             .format(user_recall_db, recall_table, self.cate_id)).groupBy('user_id') \
                            .agg(F.collect_list('movie_id').alias('movie_li')).toPandas().set_index('user_id')
        elif cf_method == 'itemCF':
            recall_table = 'trainCF'
            sim_matrix = 'train_w'
            if refit:
                recommendation = item_cf_interface(train, self.cate_id, recall_table, sim_matrix, refit)\
                    .groupBy('user_id').agg(F.collect_list('movie_id').alias('movie_li')).toPandas()\
                    .set_index('user_id')
            else:
                recommendation = u_spark.sql('select * from {}.{}_recall_{}'
                             .format(user_recall_db, recall_table, self.cate_id)).groupBy('user_id') \
                            .agg(F.collect_list('movie_id').alias('movie_li')).toPandas().set_index('user_id')
        elif cf_method == 'userCF':
            recall_table = 'user_trainCF'
            sim_matrix = 'user_train_w'
            if refit:
                recommendation = user_cf_interface(train, inverted_train, self.cate_id, recall_table, sim_matrix, refit) \
                    .groupBy('user_id').agg(F.collect_list('movie_id').alias('movie_li')).toPandas() \
                    .set_index('user_id')
            else:
                recommendation = u_spark.sql('select * from {}.{}_recall_{}'
                                             .format(user_recall_db, recall_table, self.cate_id)).groupBy('user_id') \
                    .agg(F.collect_list('movie_id').alias('movie_li')).toPandas().set_index('user_id')

        return recommendation

    def recall_precision(self, train, test, recommendation, other_topK):
        """
        召回率和精确度
        召回率和精确率有共同的分子，所以他们的相对大小也表征了 用户行为物品数量和推荐物品数量的大小关系
        :param train:
        :param test:
        :param recommendation:
        :param other_topK: 推荐结果的召回个数，other_topK <= u_topK
        :return:
        """
        hit = 0
        all_recall = 0
        all_precision = 0
        miss_test_num = 0
        for user in tqdm(train['user_id'], desc='评估召回率和精确率：'):
            try:
                tu = test['movie_dict'][user].keys()
            except:
                miss_test_num += 1
                continue
            try:
                rank = recommendation['movie_li'][user][:other_topK]
            except:
                continue
            for item in rank:
                if item in tu:
                    hit += 1
            all_recall += len(tu)
            all_precision += other_topK
        recall_rate = hit / (all_recall * 1.0)
        precision_rate = hit / (all_precision * 1.0)

        print('召回率和精确率分别为  {}%， {}%'.format(round(recall_rate * 100, 1), round(precision_rate * 100, 1)))
        print('测试集比训练集中缺失的用户数量  {}'.format(miss_test_num))   # 28138
        return recall_rate, precision_rate     # 5.1%， 1%

    def coverage(self, train, recommendation, other_topK):
        """
        覆盖率
        :param train:
        :param recommendation:
        :return:
        """
        recommend_items = set()
        all_items = set()
        no_recommend_user = 0
        for i, user in tqdm(enumerate(train['user_id']), total=len(train), desc='评估覆盖率：'):
            for item in train['movie_dict'][i].keys():
                all_items.add(item)
            try:
                rank = recommendation['movie_li'][user][:other_topK]
                for item in rank:
                    recommend_items.add(item)
            except:
                no_recommend_user += 1

        coverage_rate = len(recommend_items) / (len(all_items) * 1.0)
        print('覆盖率为  {}%'.format(round(coverage_rate * 100, 1)))      # 97.1%
        print('有行为的用户却没有推荐结果的数量： {}'.format(no_recommend_user))    # 36
        print('有行为的物品数量： {}'.format(len(all_items)))    # 27255
        return coverage_rate

    def popularity(self, train, recommendation, other_topK):
        """
        平均流行度，反向表征新颖度
        在计算平均流行度时对每个物品的流行度取对数，因为物品的流行度分布满足
        长尾分布，在取对数后，流行度的平均值更加稳定。
        :param train:
        :param test:
        :param recommendation:
        :return:
        """
        item_popularity = defaultdict(lambda: 0)
        for i in tqdm(range(len(train)), desc='统计行为物品流行度：'):
            for item in train['movie_dict'][i].keys():
                # if item not in item_popularity:
                #     item_popularity[item] = 0
                item_popularity[item] += 1
        ret = 0
        log_ret = 0
        n = 0
        for user in tqdm(train['user_id'], desc='推荐物品平均流行度：'):
            try:
                rank = recommendation['movie_li'][user][:other_topK]
                for item in rank:
                    ret += item_popularity[item]
                    log_ret += np.log(1 + item_popularity[item])
                    n += 1
            except:
                pass
        # 所以推荐物品的平均流行度
        ret /= n * 1.0
        log_ret /= n * 1.0

        print('平均流行度和对数平均流行度分别为  {}， {}'.format(round(ret, 2), round(log_ret, 2)))
        return ret, log_ret     # 1897.29,  6.06



if __name__ == '__main__':
    eval = Eval(1969)
    # train, test = eval.split_data(refit=False)
    train, test, inverted_train = eval.split_data(inverted=True, refit=False)

    # recommendation = eval.get_recommend(train.head(100), inverted_train, cf_method='userCF', refit=True)
    recommendation = eval.get_recommend(train, inverted_train, cf_method='userCF', refit=False)
    eval.recall_precision(train, test, recommendation, 100)
    eval.coverage(train, recommendation, 100)
    eval.popularity(train, recommendation, 100)

    """
    topK = 100
    # cos召回评估：
    召回率和精确率分别为  2.4%， 0.2%
    测试集比训练集中缺失的用户数量  27937
    覆盖率为  94.8%
    有行为的用户却没有推荐结果的数量： 34
    平均流行度和对数平均流行度分别为  137.45， 1.59

    # 共现召回评估：
    召回率和精确率分别为  30.0%， 1.9%
    测试集比训练集中缺失的用户数量  27937
    覆盖率为  96.9%
    有行为的用户却没有推荐结果的数量： 32
    平均流行度和对数平均流行度分别为  1905.34， 6.06

    # userCF 评估：
    召回率和精确率分别为  19.5%， 1.4%
    测试集比训练集中缺失的用户数量  27937
    覆盖率为  44.3%
    有行为的用户却没有推荐结果的数量： 25855
    有行为的物品数量： 27255
    平均流行度和对数平均流行度分别为  1815.5， 6.78

    topK = 20
    # cos召回评估：
    召回率和精确率分别为  0.5%， 0.2%
    测试集比训练集中缺失的用户数量  27937
    覆盖率为  73.4%
    有行为的用户却没有推荐结果的数量： 40
    有行为的物品数量： 27255
    平均流行度和对数平均流行度分别为  144.09， 1.53

    # 共现召回评估：
    召回率和精确率分别为  13.1%， 4.2%
    测试集比训练集中缺失的用户数量  27937
    覆盖率为  86.0%
    有行为的用户却没有推荐结果的数量： 32
    有行为的物品数量： 27255
    平均流行度和对数平均流行度分别为  3109.56， 6.97

    # userCF 评估：
    召回率和精确率分别为  7.6%， 2.8%
    测试集比训练集中缺失的用户数量  27937
    覆盖率为  21.5%
    有行为的用户却没有推荐结果的数量： 25855
    有行为的物品数量： 27255
    平均流行度和对数平均流行度分别为  3036.01， 7.39
    """



    """
    # 分割数据集，分别处理，降低单次内存使用
    train1, train2, train3, train4 = inverted_train[0:int(len(inverted_train) / 4)], \
                                     inverted_train[int(len(inverted_train) / 4):int(len(inverted_train) * 1 / 2)], \
                                     inverted_train[int(len(inverted_train) * 1 / 2):int(len(inverted_train) * 3 / 4)], \
                                     inverted_train[int(len(inverted_train) * 3 / 4):len(inverted_train)]

    # 计算 每个物品被多少用户看过的 平均值和最大值
    num = 0
    max_r = 0
    for i in inverted_train['user_dict']:
        num += len(i)
        max_r = max(max_r, len(i))
    print(num / len(inverted_train), max_r)

    C1, N1 = pre_sim_user(train1)
    C2, N2 = pre_sim_user(train2)
    C3, N3 = pre_sim_user(train3)
    C4, N4 = pre_sim_user(train4)
    C, N = merge_dict(C1, C2, C3, C4, double_dict=True), merge_dict(N1, N2, N3, N4)
    """