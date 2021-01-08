from functools import partial
from utils import MovieDataApp
from utils import userDict_path, get_stopwords_list, movie_portrait_db


# def _mapPartitions(partition, industry):
def rank_map(partition, topK):
    """
    返回textrank分词
    :param partition:
    :param topK: 【int】
    :return:
    """
    import jieba.analyse

    # 用户词典
    # jieba.load_userdict(userDict_path)
    # 所有的停用词列表
    stopwords_list = get_stopwords_list()

    class TextRank(jieba.analyse.TextRank):
        def __init__(self, window=20, word_min_len=2):
            super(TextRank, self).__init__()
            self.span = window  # 窗口大小
            self.word_min_len = word_min_len  # 单词的最小长度
            # 要保留的词性，根据jieba github ，具体参见https://github.com/baidu/lac
            self.pos_filt = frozenset(
                ('n', 'x', 'eng', 'm', 'f', 's', 't', 'nr', 'ns', 'nt', "nw", "nz", "PER", "LOC", "ORG"))

        def pairfilter(self, wp):
            """过滤条件，返回True或者False"""

            if wp.flag == "eng":
                if len(wp.word) <= 2:
                    return False

            if wp.flag in self.pos_filt and len(wp.word.strip()) >= self.word_min_len \
                    and wp.word not in stopwords_list:
                    # and wp.word.lower() not in stopwords_list:
                return True

    textrank_model = TextRank(window=10, word_min_len=2)
    allowPOS = ('n', "x", 'eng', 'm', 'nr', 'ns', 'nt', "nw", "nz", "c")

    for row in partition:
        tags = textrank_model.textrank(row.content, topK=topK, withWeight=True, allowPOS=allowPOS, withFlag=False)
        for tag in tags:
            # yield row.id, industry, tag[0], tag[1]
            yield row.song_id, row.song, row.singer, row.album, tag[0], tag[1]


def get_textrank():
    """
    返回textrank权重
    :return: 【dataframe】
    """
    spark = MovieDataApp().spark
    # 得到movie预处理数据结果
    ret = spark.sql("select * from {}.first_cut_field".format(movie_portrait_db))

    # 函数中有固定参数的时候可以用partial，直接传入此参数，返回少一个参数的函数
    # mapPartitions = partial(_mapPartitions, industry="电影")
    mapPartitions = partial(rank_map, topK=None)

    movie_tag_weights = ret.rdd.mapPartitions(mapPartitions)
    # movie_tag_weights = movie_tag_weights.toDF(["movie_id", "industry", "tag", "weights"])
    movie_tag_weights = movie_tag_weights.toDF(["song_id", "song", "singer", "album", "tag", "textrank"])

    # print(movie_tag_weights.show(10, truncate=False))
    # print(movie_tag_weights.count())
    # movie_tag_weights.where("tag='科幻'").orderBy("weights").show()

    return movie_tag_weights

# def get_topK_textrank(topK):
#     """
#     返回topK个textrank结果
#     :return: 【dataframe】
#     """
#     spark = MovieDataApp().spark
#     # 得到movie预处理数据结果
#     ret = spark.sql("select * from {}.movie_feature".format(movie_portrait_db))
#
#     mapPartitions = partial(rank_map, topK=topK)
#     movie_tag_weights = ret.rdd.mapPartitions(mapPartitions)
#     movie_tag_weights = movie_tag_weights.toDF(["movie_id", "cate_id", "tag", "textrank"])
#
#     return movie_tag_weights


if __name__ == '__main__':
    get_textrank()