from utils import m_spark
import os
import sys


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE_DIR))

channelInfo = {
            "电影": 1969,      # 45349
            "电视剧": 1970,    # 12445
            "综艺": 1971,      # 76941
            "动漫": 1972,      # 14915
            "少儿": 1973,      # 29137
            "纪录片": 1974,    # 47983
            # 以下3个channel没有数据
            # "体育": 2263,
            # "MV": 2264,
            # "健身": 2265,
            "教育": 2271,      # 800
            "知识": 2272,      # 35984
            "其他": 0,         # 2613
        }


def generate_channel_item(cate_id):
    save_path = os.path.join(BASE_DIR, "data/test/{}".format(cate_id))
    db_asset = m_spark.sql("select id, cid from movie.db_asset").toPandas()

    with open(save_path, 'w', encoding='utf-8') as f:
        for n, id in enumerate(db_asset['id'].tolist()):
            if db_asset['cid'][n] == cate_id:
                ret = str(id) + '\t' + str(db_asset['cid'][n]) + '\t' + str(id) + '\n'
                f.write(ret)


for _, cid in channelInfo.items():
    generate_channel_item(cid)