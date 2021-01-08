from sklearn.feature_extraction.text import TfidfVectorizer
from utils import stopwords
from scipy import sparse
import joblib
from setting.conf import tfidf_model, tfidf_matrix, feature_name, idf_score


def text_tfidf(data):
    """
    对中文进行特征抽取
    :param data: []
    :return: (np.ndarrary, [])
    """
    # 实例化一个转换器类
    transfer = TfidfVectorizer(stop_words=stopwords)
    # 调用fit_transform
    data = transfer.fit_transform(data)
    # print(data.shape)
    # 保存feature_name
    with open(feature_name, 'w', encoding='utf-8') as f:
        f.write(str(transfer.get_feature_names()))
    # 保存idf_array
    with open(idf_score, 'w', encoding='utf-8') as f:
        f.write(str(transfer.idf_.tolist()))
    # 保存tfidf_model
    joblib.dump(transfer, tfidf_model)
    # 保存tfidf_matrix
    sparse.save_npz(tfidf_matrix, data)  # 保存形式： sparse_matrix (numpy)



