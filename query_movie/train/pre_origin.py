import pandas as pd
import numpy as np
import re
from utils import cut
from tqdm import tqdm
from setting.conf import original_data, content_field, cut_field, cut_content


def cleaning_data():
    """
    对原数据或者录入数据进行清洗
    需要conf参数： original_data, pre_raw_data
    :return: self
    """
    data = pd.read_csv(original_data, sep='\t')
    # data = pd.read_csv(original_data, names = ['movie_id','content','year'], encoding='utf8')
    # data = pd.read_csv(original_data)
    # data.rename(columns={"id": "movie_id", "title": "content"}, inplace=True)
    # data = data.replace(to_replace=None, value=np.nan)
    # data = data.replace(to_replace='#', value=np.nan)
    # data = data.dropna()

    pat = re.compile('[a-zA-Z0-9\\u4e00-\u9fa5]+', re.S)  # 正则过滤选择所有中文、英文、数字字符
    text_list = []
    bar = data[content_field].tolist()
    total = len(data)
    for text in tqdm(bar, total=total, desc='清洗数据'):
        text = ' '.join(pat.findall(text))
        text_list.append(text)
    data[cut_field] = text_list
    # data = data.dropna()

    return data


def cut_text():
    """
    对content字段做分词处理
    :param: self.data
    :return: pd.dataframe
    """
    data = cleaning_data()
    sent_list = []
    words_li = data[cut_field].tolist()
    year = data['year'].tolist()
    bar = zip(words_li, year)
    total = len(data)
    for sent, year in tqdm(bar, total=total, desc='分词'):
        cut_words = cut(sent)
        if len(cut_words) == 0:
            if len(sent) == 0:
                words = np.nan
            else:
                words = sent + " " + str(year)
        else:
            cut_words.append(str(year))
            cut_words = list(set(cut_words))
            words = ' '.join(cut_words)
        sent_list.append(words)
    data[cut_field] = sent_list
    data = data.dropna()

    # print(data.head())
    data.to_csv(cut_content, index=False)
    return data


