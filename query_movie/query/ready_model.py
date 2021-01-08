import joblib
from setting.conf import tfidf_matrix, tfidf_model, cut_content
from scipy import sparse
import pandas as pd


class TrainReady(object):
    """
    准备训练数据tfidf权重
    """
    def __init__(self):
        # 载入模型
        self.train_tfidf = sparse.load_npz(tfidf_matrix)
        # 载入训练分词
        self.train_frame = pd.read_csv(cut_content)

    def pre_train(self):
        """
        对中文进行特征抽取
        :return: [n_samples, n_features]
        """
        return self.train_tfidf, self.train_frame


class TestReady(object):
    """
    准备测试数据tfidf权重
    """
    def __init__(self):
        # 载入模型
        self.transfer = joblib.load(tfidf_model)

    def pre_test(self):
        """
        对中文进行特征抽取
        :return: object
        """
        return self.transfer




