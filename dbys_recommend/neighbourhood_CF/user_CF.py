from collections import defaultdict
from operator import itemgetter
import pandas as pd
import gc
import numpy as np
from tqdm import tqdm
from utils import u_spark, user_portrait_db, RetToHive, user_recall_db, mCF_topK, u_topK
from functools import reduce


def get_movie_user_table(cate_id):
    try:
        inverted_table = u_spark.sql('select * from {}.movie_user_inverted_table'.format(user_portrait_db))
    except:
        inverted_table = u_spark.sql('select movie_id, title, year, cate_id, collect_list(user_id) user_li, \
                        collect_list(weight) weight_li from {}.action_weight group by movie_id, cate_id, title, year'
                         .format(user_portrait_db))
        def map(row):
            user_dict = dict(zip(row.user_li, row.weight_li))
            return row.movie_id, row.title, row.year, row.cate_id, user_dict

        inverted_table = inverted_table.rdd.map(map).toDF(['movie_id', 'title', 'year', 'cate_id', 'user_dict'])
        RetToHive(u_spark, inverted_table, user_portrait_db, 'movie_user_inverted_table')

    train = inverted_table.where('cate_id = {}'.format(cate_id)).select('movie_id', 'user_dict').toPandas()
    return train


def pre_sim_user(train):
    # action_df = train['movie_dict']
    # user_df = train['user_id']
    #
    # item_users = defaultdict(set)
    # for n, items in tqdm(enumerate(action_df), total=len(action_df), desc='构建物品用户倒排表: '):
    #     for i in items.keys():
    #         item_users[i].add(user_df[n])

    user_df = train['user_dict']

    #calculate co-rated items between users
    C = defaultdict(lambda: defaultdict(lambda: 0))
    N = defaultdict(lambda :0)
    for user_dict in tqdm(user_df, desc='相似度中间参数计算：'):
        user_num = len(user_dict)
        users = user_dict.keys()
        # 一方面降低计算的时间复杂度，一方面本身热门电影对用户相似度贡献也比较低
        # 每个物品被多少用户看过：平均值 61.617978352595856  最大值 15515
        # 暂定 选择热度大于 500 的电影不参与用户相似权重计算
        if user_num <= 1 or user_num > 500:
            continue
        # 物品热度惩罚
        cost_active_item = np.log(1 + user_num * 1.0)
        for u in users:
            N[u] += 1
            for v in users:
                if u == v:
                    continue
                C[u][v] += round(1 / cost_active_item, 5)
    return C, N


def merge_dict(*args, double_dict=False):
    """
    合并字典，并对相同键的值求和
    :param args:
    :param double_dict:  为True时， 是对字典嵌套字典进行处理
    :return:
    """
    def sum_dict(a,b):
        temp = dict()
        for key in a.keys() | b.keys(): # 并集
            temp[key] = sum([d.get(key, 0) for d in (a, b)])
        return temp

    def sum_double_dict(a,b):
        temp = defaultdict(dict)
        for key in a.keys() | b.keys(): # 并集
            for i in a[key].keys() | b[key].keys():
                temp[key][i] = sum([d[key].get(i, 0) for d in (a, b)])
        return temp

    if double_dict:
        ret = reduce(sum_double_dict, [*args])
    else:
        ret = reduce(sum_dict, [*args])

    return ret


def UserSimilarity(train, sim_matrix):
    """
    :param train:  物品用户倒排表
    :param sim_matrix:
    :return:
    """
    C,  N = pre_sim_user(train)
    #calculate finial similarity matrix W
    max_w = 0
    W = defaultdict(dict)
    for u, related_users in tqdm(C.items(), desc='相似度矩阵计算：'):
        for v, cuv in related_users.items():
            W[u][v] = round(cuv / np.sqrt(N[u] * N[v]), 5)
            max_w = max(max_w, W[u][v])

    sort_w = dict()
    for user_id in tqdm(C.keys(), desc='相似度矩阵排序：'):
        sort_w[user_id] = sorted(C[user_id].items(), key = itemgetter(1), reverse = True)[: mCF_topK]

    sort_w['max'] = max_w
    np.save('../data/rating_matrix/{}.npy'.format(sim_matrix), sort_w)
    return sort_w


def user_cf_interface(train_df, inverted_df, cate_id, recall_table, sim_matrix, refit):
    """
    :param train_df:   用户行为表
    :param inverted_df:   物品用户倒排表
    :param cate_id:
    :param recall_table:
    :param sim_matrix:
    :param refit:
    :return:
    """
    if refit:
        sort_w = UserSimilarity(inverted_df, sim_matrix)
    else:
        sort_w = np.load('../data/rating_matrix/{}.npy'.format(sim_matrix), allow_pickle=True).item()
    # sort_w = np.load('../data/rating_matrix/{}.npy'.format(sim_matrix), allow_pickle=True).item()

    train_df = train_df.set_index('user_id')
    max_r = sort_w['max']
    sort_w.pop('max')
    ret = []
    # 遍历所有用户
    for u, ru in tqdm(sort_w.items(), desc='userCF: '):
        u_action = train_df['movie_dict'][u].keys()
        rank = defaultdict(lambda: 0)
        relation = defaultdict(list)
        # 遍历每个用户的所有相似用户
        for v, pi in ru:
            try:
                # 遍历当前用户的所有行为物品
                for j, wj in train_df['movie_dict'][v].items():
                    if j in u_action:
                        continue
                    normal_weight = pi * wj / max_r
                    rank[j] += normal_weight   # 用户行为权重乘以物品相似度
                    # relation[j][i] = pi * wj / max_r
                    relation[j] += [(v, normal_weight)]
            except:
                pass
        rank = sorted(rank.items(), key=itemgetter(1), reverse=True)[:u_topK]
        for i, r in rank:
            # 因为sort_w是排好序的，所以relation已完成排序
            relation_movie = relation[i][:5]
            ret.append([u, i, r, str(relation_movie)])

    del sort_w
    gc.collect()

    # cf_recall_table = 'itemCF_recall_{}'.format(cate_id)
    cf_recall_table = '{}_recall_{}'.format(recall_table, cate_id)
    df = pd.DataFrame(ret, columns=['user_id', 'movie_id', 'weight', 'relation'])
    sp_df = u_spark.createDataFrame(df)
    RetToHive(u_spark, sp_df, user_recall_db, cf_recall_table)

    return sp_df