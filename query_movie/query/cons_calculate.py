from sklearn.metrics.pairwise import cosine_similarity
from setting.conf import id_field, content_field, cut_field
import pandas as pd
from tqdm import tqdm


def similar_calculate(train_tfidf, test_tfidf):
    """
    :param train_tfidf: X: (n_samples, n_features)
    :param test_tfidf:  Y: (n_samples, n_features)
    :return: [object,..]
    """
    # print('计算余弦相似度....')
    # k: ndarray(train_tfidf_len, test_tfidf_len) <=> (n_samples_X, n_samples_Y)
    k = cosine_similarity(train_tfidf, test_tfidf)
    df = pd.DataFrame(k)
    # print(df.head())
    sort_list = []
    for i in range(df.shape[1]):
        series = pd.Series(df[i])
        # item()对象：{排序的index：similar}
        similar_sort = series.sort_values(ascending=False).head(5).items()
        # similar_sort = series.sort_values(ascending=False).head(1).items()
        sort_list.append(similar_sort)
    # print('计算完成')
    return sort_list


def generate_eval(sort_list, test_frame, train_frame):
    """
    :param sort_list:
    :param train_frame:
    :param test_frame:
    :return: {master_id:[(recall_id,similar),..]}
    """
    # print('生成相似度结果文件....')
    result = []
    tag = len(sort_list)
    if tag >= 1:
        for i, item in tqdm(enumerate(sort_list), total=tag, desc='匹配电影id和相似度：'):
            try:
                master_id = int(test_frame[id_field][i])
                master_words = test_frame.loc[test_frame.index[i], [cut_field]].tolist()[0].split()
                # print(master_words)
            except Exception as e:
                continue
            sim_li = []
            for j, similar in item:
                try:
                    similar = float('%.4f' % similar)
                    recall_id = int(train_frame.loc[train_frame.index[j], [id_field]])
                    recall_title = train_frame.iloc[j, train_frame.columns.get_indexer([content_field])].tolist()[0]
                    recall_words = train_frame.iloc[j, train_frame.columns.get_indexer([cut_field])].tolist()[0]
                    # print(recall_words)
                    match = 0   # 匹配到的分词个数
                    no_match = 0  # 未匹配到的分词个数
                    for word in recall_words.split():
                        if word in master_words:
                            match += 1
                            continue
                        # if word.isdigit() and len(word) == 1:
                        # if word.isdigit():
                        #     # print(word)
                        #     no_match -= 1
                        # if word.isalpha():
                        #     no_match -= 1
                        no_match -= 1
                        # print(word)
                    sim_li.append((match, no_match, similar, recall_id, recall_title))
                except Exception as e:
                    continue
            sim_li = sorted(sim_li, reverse=True)
            result.append((master_id, sim_li))
            # result.append((master_id, sim_li[0][3:]))
    # print('生成完毕')
    return result
