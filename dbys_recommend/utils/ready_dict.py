import os
import codecs
import sys


# BASE_DIR = os.path.dirname(os.getcwd())
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE_DIR))


# 结巴原生词典
originDict_path = os.path.join(BASE_DIR, "data/data_dicts/dict.txt.big")
# 结巴加载用户词典
userDict_path = os.path.join(BASE_DIR, "data/data_dicts/user_dict.txt")
# 结巴加载零时用户词典（用于title分词过滤召回结果）
tmpDict_path = os.path.join(BASE_DIR, "data/data_dicts/tmp_dict.txt")
# 停用词文本
stopwords_path = os.path.join(BASE_DIR, "data/stopwords/all_stopwords.txt")


def get_stopwords_list():
    """返回stopwords列表"""
    stopwords_list = [i.strip()
                      for i in codecs.open(stopwords_path).readlines()]
    return stopwords_list