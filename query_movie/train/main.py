from train.pre_origin import cut_text
from setting.conf import cut_field
from train.tfidf_train import text_tfidf
import os
import sys


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

def run():
    # 清洗数据并分词
    train_frame = cut_text()
    # 计算tfidf权重
    print('计算训练数据tfidf....')
    input_list = train_frame[cut_field].tolist()
    text_tfidf(input_list)
    print('计算完成')


if __name__ == '__main__':
    run()