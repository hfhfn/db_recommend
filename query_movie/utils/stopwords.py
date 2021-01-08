"""获取停用词"""
from setting.conf import stopwords_path


stopwords = [i.strip() for i in open(stopwords_path, encoding='utf-8').readlines()]

