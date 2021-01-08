"""
基本参数
"""
import os
import sys


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

# 停用词路径
stopwords_path = os.path.join(BASE_DIR, "data/stopwords/stopwords.txt")
# 分词content字段
content_field = 'content'
# 分词后字段
cut_field = 'words'
# 电影id字段
id_field = 'movie_id'

# 训练数据的tfidf矩阵
tfidf_matrix = os.path.join(BASE_DIR, "data/train_tfidf.npz")

# tfidf模型文件
tfidf_model = os.path.join(BASE_DIR, 'model/tfidf_model.pkl')

# 分词数据
cut_content = os.path.join(BASE_DIR, 'data/cut_content.csv')

# 原数据路径
original_data = os.path.join(BASE_DIR, 'data/origin_data/db_asset.csv')

# feature_name
feature_name = os.path.join(BASE_DIR, 'data/feature_name.txt')

# idf_score
idf_score = os.path.join(BASE_DIR, 'data/idf_score.txt')
