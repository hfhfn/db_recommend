import os
import sys

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(BASE_DIR))

from content_recall.update import train_movie_profile, train_user_profile, train_recall, train_factor, train_history

"""
脚本传参顺序：
1. full:  (False / True)   必传
2. profile:  (user / movie)
3. channel:  ('电影' ..)
4. cate_id:  (1969 ..)
"""

def train():
    # full = 'false'
    # param = 'movie'
    # channel = '电影'
    # cate_id = 1969
    # 第一个参数传递是否为全量或增量计算
    full = sys.argv[1].lower()
    if full == 'false':
        full = False
    else:
        full = True
    param = sys.argv[2]
    if param == 'movie':
        train_movie_profile(full)
    elif param == 'user':
        train_user_profile(full)
    elif param == 'factor':
        train_factor()
    elif param == 'history':
        train_history(full)
    else:
        channel = sys.argv[2]
        cate_id = int(sys.argv[3])
        train_recall(channel, cate_id)


if __name__ == '__main__':
    train()
