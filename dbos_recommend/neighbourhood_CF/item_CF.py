from functools import partial
from tqdm import trange
import time
import multiprocessing
from pyspark import StorageLevel
from operator import itemgetter
from collections import defaultdict
from tqdm import tqdm
import pandas as pd
import numpy as np
from utils import u_spark, user_portrait_db, u_topK, mCF_topK, RetToHive, user_recall_db, movie_original_db
import gc


def save_cf_action_weight():
    action_weight = u_spark.sql('select user_id, cate_id, collect_list(movie_id) movie_li, \
                            collect_list(weight) weight_li from {}.action_weight \
                            group by user_id, cate_id'.format(user_portrait_db))

    def map(row):
        movie_dict = dict(zip(row.movie_li, row.weight_li))
        return row.user_id, row.cate_id, movie_dict

    action_weight = action_weight.rdd.map(map).toDF(['user_id', 'cate_id', 'movie_dict'])
    RetToHive(u_spark, action_weight, user_portrait_db, 'cf_action_weight')
    return action_weight


def ItemSimilarity(train, sim_matrix):
    N = defaultdict(lambda: 0)  # 物品计数 （即此电影被多少用户看过）
    C = defaultdict(lambda: defaultdict(lambda: 0))  # 物品-物品-共现矩阵
    # 以上 C 定义嵌套字典的方式 无法使用np.save保存，以下 W 定义方式可以保存
    W = defaultdict(dict)  # 物品-物品-相似度矩阵
    #calculate co-rated users between items
    # movie_df = train['movie_dict']
    for movie_dict in tqdm(train, desc="相似度中间参数计算："):
        movie_num = len(movie_dict)
        if movie_num <= 1:
            pass
        else:
            keys = movie_dict.keys()
            # 用户活跃度惩罚
            cost_active_user = np.log(1 + movie_num * 1.0)
            for i in keys:
                N[i] += 1
                for j in keys:
                    if i == j:
                        continue
                    # math.log(1 + len(keys) * 1.0) 对活跃用户进行惩罚，降低活跃用户观看电影之间的相似度权重
                    C[i][j] += 1 / cost_active_user
    #calculate finial similarity matrix W
    max_w = 0
    for i, related_items in tqdm(C.items(), desc="相似度矩阵计算："):
        for j, cij in related_items.items():
            W[i][j] = cij / np.sqrt(N[i] * N[j])
            # 获得最大的相似度，用于归一化
            max_w = max(max_w, W[i][j])

    sort_w = dict()
    for movie_id in tqdm(W.keys(), desc='相似度矩阵排序：'):
        sort_w[movie_id] = sorted(W[movie_id].items(), key = itemgetter(1), reverse = True)[: mCF_topK]
    sort_w['max'] = max_w
    np.save('../data/rating_matrix/{}.npy'.format(sim_matrix), sort_w)
    return  sort_w


def item_cf_interface(train_df, cate_id, recall_table, sim_matrix, refit):
    # train_df = u_spark.sql('select * from {}.cf_action_weight'.format(user_portrait_db))\
    #                             .where('cate_id={}'.format(cate_id)).drop('cate_id')#.limit(1000)
    # train_df = train_df.toPandas()#.set_index('user_id')

    movie_df = train_df['movie_dict']
    user_df = train_df['user_id']

    # del train_df
    # gc.collect()

    if refit:
        sort_w = ItemSimilarity(movie_df, sim_matrix)
    else:
        sort_w = np.load('../data/rating_matrix/{}.npy'.format(sim_matrix), allow_pickle=True).item()

    max_r = sort_w['max']
    sort_w.pop('max')
    ret = []
    # 遍历所有用户
    for n, ru in tqdm(enumerate(movie_df), total=len(movie_df), desc='itemCF: '):
        user_id = user_df[n]
        rank = defaultdict(lambda: 0)
        # relation = defaultdict(dict)
        relation = defaultdict(list)
        # 遍历每个用户的所有行为
        for i, pi in ru.items():
            try:
                for j, wj in sort_w[i]:
                    if j in ru.keys():
                        continue
                    normal_weight = pi * wj / max_r
                    rank[j] += normal_weight   # 用户行为权重乘以物品相似度
                    # relation[j][i] = pi * wj / max_r
                    relation[j] += [(i, normal_weight)]
            except:
                pass
        rank = sorted(rank.items(), key=itemgetter(1), reverse=True)[:u_topK]
        for i, r in rank:
            # relation_movie = sorted(relation[i].items(), key=itemgetter(1), reverse=True)[:5]
            # 因为sort_w是排好序的，所以relation已完成排序
            relation_movie = relation[i][:5]
            ret.append([user_id, i, r, str(relation_movie)])

    del movie_df
    del user_df
    del sort_w
    gc.collect()

    # cf_recall_table = 'itemCF_recall_{}'.format(cate_id)
    cf_recall_table = '{}_recall_{}'.format(recall_table, cate_id)
    df = pd.DataFrame(ret, columns=['user_id', 'movie_id', 'weight', 'relation'])
    sp_df = u_spark.createDataFrame(df)
    RetToHive(u_spark, sp_df, user_recall_db, cf_recall_table)

    return sp_df


def ItemCosineSimilarity(train, sim_matrix):
    """
    :param train:  所有用户的历史行为
    :return:
    """
    mu = defaultdict(lambda: 0)
    dot = defaultdict(lambda: defaultdict(lambda: 0))
    cos = defaultdict(dict)  # 物品-物品-相似度矩阵
    # i_total / i_num 求得每个物品的平均分
    i_total = defaultdict(lambda: 0)  # 每个物品获得的总分
    i_num = defaultdict(lambda: 0)   # 每个物品被多少用户打分了
    # score = 0
    # 用户数量
    # u_num = 0
    for movie_dict in tqdm(train, desc="相似度中间参数计算："):
        # 每个用户对他有行为的物品的平均分
        u_mean = np.mean(list(movie_dict.values()))
        if len(movie_dict) <= 1:
            pass
        else:
            items = movie_dict.items()
            for i, ri in items:
                i_total[i] += ri
                i_num[i] += 1
                mu[i] += (ri - u_mean) ** 2
                for j, rj in items:
                    if i == j:
                        continue
                    dot[i][j] += (ri - u_mean) * (rj - u_mean)
        # u_num += 1
        # score += r_mean
    # 所有物品平均分
    # score_mean = score / u_num
    #calculate finial similarity matrix
    for i, related_items in tqdm(dot.items(), desc="相似度矩阵计算："):
        for j, dotij in related_items.items():
            d = np.sqrt(mu[i] * mu[j])
            # if d == 0:
            #     cos[i][j] = 0
            # else:
            #     cos[i][j] = round(dotij / d, 8)
            try:
                cos[i][j] = round(dotij / d, 8)
            except:
                cos[i][j] = 0

    sort_cos = dict()
    for movie_id in tqdm(cos.keys(), desc='相似度矩阵排序：'):
        # 把相似度排序结果和平均分以元组的方式一起存储
        sort_cos[movie_id] = (sorted(cos[movie_id].items(), key = itemgetter(1), reverse = True)[: mCF_topK], \
                              round(i_total[movie_id] / i_num[movie_id], 8))
    # sort_cos['r_mean'] = score_mean
    np.save('../data/rating_matrix/{}.npy'.format(sim_matrix), sort_cos)
    return  sort_cos


# 使用字典实现评分矩阵
def item_cos_cf_interface(train_df, cate_id, recall_table, sim_matrix, refit):
    # action_weight = u_spark.sql('select * from {}.cf_action_weight'.format(user_portrait_db))\
    #                             .where('cate_id={}'.format(cate_id)).drop('cate_id')#.limit(1000)
    # train_df = action_weight.toPandas()

    # del action_weight
    # gc.collect()

    action_df = train_df['movie_dict']
    user_df = train_df['user_id']

    # del train_df
    # gc.collect()

    # movie_df = u_spark.sql('select id from {}.db_asset'.format(movie_original_db))\
    #                                             .where('cid = {}'.format(cate_id)).toPandas()
    # np.load 通过 item() 才能载入字典格式
    if refit:
        sort_cos = ItemCosineSimilarity(action_df, sim_matrix)
    else:
        sort_cos = np.load('../data/rating_matrix/{}.npy'.format(sim_matrix), allow_pickle=True).item()

    # r_mean = sort_cos['r_mean']
    # sort_cos.pop('r_mean')
    # print(len(cos.keys()))   # 28671

    ret = []
    # 遍历所有用户
    for i, ru in tqdm(enumerate(action_df), total=len(action_df), desc='cos_itemCF: '):
        user_id = user_df[i]
        # 初始化预测用户对影视打分字典
        r_predict = dict()
        relation = defaultdict(list)
        # 遍历所有电影,分别对其进行评分预测
        # for movie_id in movie_df['id']:
        for movie_id in sort_cos.keys():
            n = 0  # 分子
            d = 0  # 分母
            i_mean = sort_cos[movie_id][1]
            # 遍历 movie_id 的 mCF_topK 个相似电影
            for j, wj in sort_cos[movie_id][0]:
                if j not in ru.keys():
                    continue
                relation[movie_id] += [(j, wj)]
                n += wj * (ru[j] - i_mean)
                d += abs(wj)
            # if d == 0:
            #     continue
            try:
                r_predict[movie_id] = i_mean + n / d
            except:
                pass
        predict = sorted(r_predict.items(), key=itemgetter(1), reverse=True)[:u_topK]
        for movie_id, weight in predict:
            relation_movie = relation[movie_id][:5]
            ret.append([user_id, movie_id, weight, str(relation_movie)])

    # del movie_df
    del user_df
    del action_df
    gc.collect()

    cos_cf_recall_table = '{}_recall_{}'.format(recall_table, cate_id)  # item_cos_cf
    df = pd.DataFrame(ret, columns=['user_id', 'movie_id', 'weight', 'relation'])
    sp_df = u_spark.createDataFrame(df)
    RetToHive(u_spark, sp_df, user_recall_db, cos_cf_recall_table)

    return sp_df


# spark计算引擎内存不足
"""
def get_cos_cf_recommend(cate_id):
    action_weight = u_spark.sql('select * from {}.cf_action_weight'.format(user_portrait_db))\
                                .where('cate_id={}'.format(cate_id)).drop('cate_id')#.limit(1000)
    # train_df = action_weight.toPandas()
    # ItemCosineSimilarity(train_df)
    movie_df = u_spark.sql('select id from {}.db_asset'.format(movie_original_db))\
                                                .where('cid = {}'.format(cate_id)).toPandas()
    cos = np.load('../data/rating_matrix/cos.npy', allow_pickle=True).item()
    r_mean = cos['r_mean']['r_mean']

    def Recommendation(partition):
        n = 0  # 分子
        d = 0  # 分母
        for row in partition:
            r_predict = defaultdict(lambda: defaultdict(lambda: 0))
            for movie_id in movie_df['id']:
                for j, wj in sorted(cos[movie_id].items(), key=itemgetter(1), reverse=True)[:mCF_topK]:
                    if j in row.movie_dict.keys():
                        continue
                    n += wj * (row.movie_dict[j] - r_mean)
                    d += abs(wj)
                r_predict[row.user_id][movie_id] = r_mean + n / d

            for movie_id, weight in sorted(r_predict[row.user_id].items(), key=itemgetter(1), reverse=True)[:u_topK]:
                relation_movie = sorted(cos[movie_id].items(), key=itemgetter(1), reverse=True)[:5]
                yield row.user_id, movie_id, weight, relation_movie

    action_weight = action_weight.limit(1)
    recommend_df = action_weight.rdd.persist(storageLevel=StorageLevel.MEMORY_AND_DISK).repartition(1000)\
                        .mapPartitions(Recommendation).toDF(['user_id', 'movie_id', 'weight', 'relation'])
    del action_weight
    gc.collect()
    # recommend_df.show(truncate=False)
    RetToHive(u_spark, recommend_df, user_recall_db, 'item_cos_cf_recall_{}'.format(cate_id))
"""


# spark计算引擎内存不足
"""
def get_cf_recommend(cate_id):
    action_weight = u_spark.sql('select * from {}.cf_action_weight'.format(user_portrait_db))\
                                .where('cate_id={}'.format(cate_id)).drop('cate_id')#.limit(1000)
    # train_df = action_weight.toPandas()#.set_index('user_id')
    # action_weight.show()
    # ItemSimilarity(train_df)

    # del train_df
    # gc.collect()

    W = np.load('../data/rating_matrix/W.npy', allow_pickle=True).item()
    max_w = W['max']['max']

    def Recommendation(partition):
        for row in partition:
            rank = defaultdict(lambda: 0)
            relation = defaultdict(lambda: defaultdict(lambda: 0))
            for i, pi in row.movie_dict.items():
                for j, wj in sorted(W[i].items(), key=itemgetter(1), reverse=True)[:mCF_topK]:
                    if j in row.movie_dict.keys():
                        continue
                    recommend_weight = pi * wj / max_w
                    rank[j] += recommend_weight # 用户行为权重乘以物品相似度
                    relation[j][i] = recommend_weight
            # yield row.user_id, rank, relation
            for movie_id, weight in sorted(rank.items(), key=itemgetter(1), reverse=True)[:u_topK]:
                relation_movie = sorted(relation[movie_id].items(), key=itemgetter(1), reverse=True)[:5]
                yield row.user_id, movie_id, weight, relation_movie

    # withReplacement：是否有放回的采样
    # fraction：采样比例
    # seed：随机种子
    # action_weight = action_weight.sample(withReplacement = False, fraction = 0.01, seed = 100)
    action_weight = action_weight.limit(1)

    recommend_df = action_weight.rdd.persist(storageLevel=StorageLevel.MEMORY_AND_DISK).repartition(1000)\
                        .mapPartitions(Recommendation).toDF(['user_id', 'movie_id', 'weight', 'relation'])
    del action_weight
    gc.collect()
    # recommend_df.show(truncate=False)
    RetToHive(u_spark, recommend_df, user_recall_db, 'item_cf_recall_{}'.format(cate_id))
"""


# 多进程计算，更快，但内存吃不消，  计算密集型用多进程，IO密集型用多线程
# 可以先拆分数据集，再分别计算，最后合并结果
"""
def run2(user_df, movie_df, W, max_r, ret, queue, m):
    for n, ru in tqdm(enumerate(movie_df), total=len(movie_df), desc='第-{}-组itemCF: '.format(m)):
        rank = defaultdict(lambda: 0)
        relation = defaultdict(dict)
        for i, pi in ru.items():
            for j, wj in sorted(W[i].items(), key=itemgetter(1), reverse=True)[0:mCF_topK]:
                if j in ru.keys():
                    continue
                rank[j] += pi * wj / max_r   # 用户行为权重乘以物品相似度
                relation[j][i] = pi * wj / max_r
        rank = sorted(rank.items(), key=itemgetter(1), reverse=True)[:u_topK]
        for i, r in rank:
            relation_movie = sorted(relation[i].items(), key=itemgetter(1), reverse=True)[:5]
            ret.append([user_df.iloc[n], i, r, str(relation_movie)])
    # queue.put(user_id)
    queue.send(ret)
    queue.close()


def interface2(cate_id):
    action_weight = u_spark.sql('select * from {}.cf_action_weight'.format(user_portrait_db))\
                                .where('cate_id={}'.format(cate_id)).drop('cate_id')#.limit(1000)
    train_df = action_weight.toPandas()#.set_index('user_id')
    # 数据分桶，未使用
    # train_df['tag'] = range(len(train_df))
    # train_df['seg'] = pd.qcut(train_df['tag'], 8, labels=[1, 2, 3, 4, 5, 6, 7, 8])

    W = np.load('../data/rating_matrix/W.npy', allow_pickle=True).item()
    max_r = W['max']['max']

    # 大概是因为爆内存的问题，进程池不如multiprocessing.Process的多进程效率高（容易报错）弃用进程池
    # pool = multiprocessing.Pool(processes = multiprocessing.cpu_count())
    # pool = multiprocessing.Pool(processes = 6)
    # 进程间通信 pipe或者queue
    # queue = multiprocessing.Manager().Queue()
    con1, con2 = multiprocessing.Pipe()   # con1，con2 管道的两端，前一个发送，后一个接收
    user_num = len(train_df)
    ret = []
    # ps = []
    for m, i in enumerate(range(int(len(train_df) / 10000) + 1)):
        seg_train = train_df[i: i + 10000]
        user_df = seg_train['user_id']
        movie_df = seg_train['movie_dict']
        # pool.apply_async(run2, args=(user_df, movie_df, W, max_r, ret, con1, m))
        p = multiprocessing.Process(target=run2, args=(user_df, movie_df, W, max_r, ret, con1, m))
        p.start()
        # ps.append(p)

    # pool.close()
    # pool.join()
    for i in tqdm(range(user_num), desc="进程执行进度："):
        ret += con2.recv()
        print("\r这是第-%s-个用户 ：" % i, end="")

    # for p in ps:
    #     p.join()

    # tqdm的不同使用方式，不成功
    # with trange(len(train_df)) as pbar:
    #     for i in pbar:
    # with tqdm(len(train_df)) as pbar:
    #     for i in range(len(train_df)):
    #         # 设置进度条左边显示的信息
    #         pbar.set_description("进程执行进度：%s" % i)
    #         q = queue.get()
    #         # 设置进度条右边显示的信息
    #         pbar.set_postfix(user_id = q)
    #         # 每次更新进度条的长度
    #         pbar.update(1)

    # 替代tqdm方式，监视进程进度
    # train_ok = 0
    # while True:
    #     # user_id = queue.get()
    #     user_id = con2.recv()
    #     # con2.close()
    #     # print("已经完成train: %s" % user_id)
    #     train_ok += 1
    #     time.sleep(0.1)
    #     print("\r训练的进度为：%.2f %%" % (train_ok * 100 / user_num), end="")
    #
    #     if train_ok == user_num:
    #         break

    df = pd.DataFrame(ret, columns=['user_id', 'movie_id', 'weight', 'relation'])
    sp_df = u_spark.createDataFrame(df)
    RetToHive(u_spark, sp_df, user_recall_db, 'item_cf_recall_{}'.format(cate_id))
    # return sp_df
"""




if __name__ == '__main__':
    # get_cf_recommend(1969)
    # get_cos_cf_recommend(1969)
    # interface2(1969)
    # cos_cf_interface(1969)
    # cf_interface(1969)#'4C:91:7A:5D:71:74',
    pass


