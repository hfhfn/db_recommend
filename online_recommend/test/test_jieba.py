# encoding=utf-8

# text = "小杨在贵州财经大学工作，擅长大数据、云计算，喜欢乾清宫、黄果树瀑布等景区。"
# text = "美国队长2普通话版"
#
# # 导入自定义词典
# # jieba.load_userdict("dict.txt")
#
# #添加自定义词语
# jieba.add_word("小杨")
# jieba.add_word("黄果树瀑布")
# #调节词频
# jieba.suggest_freq('大数据', True)
#
# #精确模式
# data = jieba.cut(text, cut_all=False)
# print(u"[原始文本]: ", text, "\n")
# print(u"[精确模式]: ", " ".join(data), "\n")
# data2 = jieba.lcut(text)
# print(u"lcut方法：", data2)
#
# #词性标注
# sentence_seged = jieba.posseg.cut(text)
# outstr = ''
# for x in sentence_seged:
#     outstr += "{}/{}  ".format(x.word, x.flag)
# print(u'[词性标注]:', outstr)

# from test_createDataFrame import data
# from movie_portrait import FilterCutWords
# recall_df = FilterCutWords().get_words(data)
# recall_df.show()

from utils.default import m_spark
from utils.cut_words import BaseCutWords
from utils.save_tohive import RetToHive

test_db = 'test'


class QueryCutWords(BaseCutWords):
    def segmentation(self, partitions):
        for row in partitions:
            words = self.cut_sentence(row.title)
            year = str(row.year)
            if len(words) == 0:
                words = [row.title, year]
            else:
                # 列表的append方法在spark partition中不能识别（还有set命令，也不能识别）
                words = words + [year]
            yield row.id, row.title, words

    def get_words(self, sentence_df=None, add_year=False):
        """
        返回分词结果
        :return: 【dataframe】
        """

        def not_year_segmentation(partitions):
            for row in partitions:
                yield row.id, row.title, self.cut_sentence(row.title)

        if add_year:
            doc = sentence_df.rdd.mapPartitions(self.segmentation)
            doc = doc.toDF(["movie_id", "title", "words"])
        else:
            doc = sentence_df.rdd.mapPartitions(not_year_segmentation)
            doc = doc.toDF(["query_id", "query_title", "words"])
        return doc


def save_cut_words(dataframe, table_name, add_year=True):
    cut = QueryCutWords().get_words(dataframe, add_year)  # .show(100, truncate=False)
    cut_table = 'cut_{}'.format(table_name)
    RetToHive(m_spark, cut, test_db, cut_table)


def save_query_result(dataframe, table_name):
    cut_table = 'query_{}'.format(table_name)
    RetToHive(m_spark, dataframe, test_db, cut_table)


"""
欲望都市I.Sex.And.The.City.1.2008.BluRay.720p.x264.AC3-CMCT.mkv
[BD影视分享bd-film.cc]怒火救援.Man.on.Fire.2004.全屏版.DDP5.1.HD1080P.国英双语.中英双字.mkv
[BD影视分享bd-film.cc]终结者：黑暗命运.Terminator.Dark.Fate.2019.BD1080P.中英双字
[变形金刚5：最后的骑士].Transformers.The.Last.Knight.2017.3D.BluRay.1080p.HSBS.x264.TrueHD7.1-CMCT
的士速递3..720p.BD国语中英双字[最新电影www.66ys.tv]
"""

import os
import sys
import re
from pyspark.sql import functions as F

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE_DIR))
title_path = os.path.join(BASE_DIR, "data/test/movie_title")


def tokenlize(sentence):
    # filters = ['!', '"', '#', '$', '%', '&', '\(', '\)', '\*', '\+', ',', '-', '\.', '/', ':', ';', '<', '=', '>',
    #             '\?', '@', '\[', '\\', '\]', '^', '_', '`', '\{', '\|', '\}', '~', '\t', '\n', '\x97', '\x96', '”', '“', ]
    filters = ['\.']
    # sentence = sentence.lower() #把大写转化为小写
    # sentence = re.sub("<br />"," ",sentence)
    # sentence = re.sub("I'm","I am",sentence)
    # sentence = re.sub("isn't","is not",sentence)
    result = re.sub("|".join(filters), " ", sentence)
    # result = [i for i in sentence.split(" ") if len(i)>0]

    return result


def save_cut_movie():
    movie_df = m_spark.sql('select id, title, year from movie.db_asset where cid=1969')
    save_cut_words(movie_df, 'title', True)


def save_cut_query(sentence):
    # with open(title_path, 'r', encoding='utf-8') as f:
    #     title_li = [tokenlize(i) for i in f.readlines()]
    #     # 读取为列表，并去掉 \n
    #     # title_li = f.read().splitlines()
    #     title_index = range(len(title_li))
    #     title_data = list(zip(title_index, title_li))

    if isinstance(sentence, str):
        title_data = [0, tokenlize(sentence)]
    else:
        title_li = [tokenlize(i) for i in sentence]
        title_index = range(len(title_li))
        title_data = list(zip(title_index, title_li))

    query_df = m_spark.createDataFrame(title_data, schema=['id', 'title'])
    # query_df = query_df.withColumn('title', query_df['summary'])

    save_cut_words(query_df, 'query', False)


def query_movie():
    title_df = m_spark.sql("select * from test.cut_title").withColumn('word', F.explode('words')) \
        .drop('words').dropDuplicates()

    query_df = m_spark.sql("select movie_id query_id, title query_title, words from test.cut_query") \
        .withColumn('word', F.explode('words')).drop('words').dropDuplicates()

    join_df = query_df.join(title_df, on='word', how='inner')
    # join_df.show()

    count_df = join_df.groupby("query_id", "movie_id").agg(F.count('*').alias('num'))

    df = join_df.join(count_df, on=['query_id', 'movie_id'])

    df.registerTempTable("tempTable")
    result = m_spark.sql(
        "select query_id, movie_id, query_title, title, collect_list(word) words, num \
                    from tempTable group by query_id, movie_id, query_title, title, num")

    def del_full_num(row):
        word = ' '.join(row.words)
        zhmodel = re.compile(u'[\u4e00-\u9fa5]')
        if zhmodel.search(word):
            return row.query_id, row.movie_id, row.query_title, row.title, row.words, row.num
        else:
            return None, None, None, None, None, None

    result = result.rdd.map(del_full_num).toDF(
        ["query_id", "movie_id", "query_title", "title", "words", "num"]).dropna()

    from pyspark.sql import Window
    result = result.withColumn("sort",
                               F.row_number().over(Window.partitionBy("query_id").orderBy(
                                   result["num"].desc())))  # .where('sort = 1')

    save_query_result(result, 'result')


def query_online(sentence):
    if isinstance(sentence, str):
        num = 1
        title_data = [(0, tokenlize(sentence))]
    else:
        num = len(sentence)
        title_li = [tokenlize(i) for i in sentence]
        title_index = range(len(title_li))
        title_data = list(zip(title_index, title_li))

    query_df = m_spark.createDataFrame(title_data, schema=['id', 'title'])
    cut = QueryCutWords().get_words(query_df, False)
    query_df = cut.withColumn('word', F.explode('words')).drop('words').dropDuplicates()

    title_df = m_spark.sql("select * from test.cut_title").withColumn('word', F.explode('words')) \
        .drop('words').dropDuplicates()

    join_df = query_df.join(title_df, on='word', how='inner')
    count_df = join_df.groupby("query_id", "movie_id").agg(F.count('*').alias('num'))
    df = join_df.join(count_df, on=['query_id', 'movie_id'])
    df.registerTempTable("tempTable")
    result = m_spark.sql(
        "select query_id, movie_id, query_title, title, collect_list(word) words, num \
                    from tempTable group by query_id, movie_id, query_title, title, num")

    def del_full_num(row):
        word = ' '.join(row.words)
        zhmodel = re.compile(u'[\u4e00-\u9fa5]')
        if zhmodel.search(word):
            return row.query_id, row.movie_id, row.query_title, row.title, row.words, row.num
        else:
            return None, None, None, None, None, None

    result = result.rdd.map(del_full_num).toDF(
        ["query_id", "movie_id", "query_title", "title", "words", "num"]).dropna()
    from pyspark.sql import Window
    result = result.withColumn("sort",
                               F.row_number().over(Window.partitionBy("query_id").orderBy(
                                   result["num"].desc()))).where('sort = 1').select('movie_id', 'title')

    json = result.toJSON().take(num)
    print(json)
    print(type(json))


if __name__ == '__main__':
    # save_cut_test()
    # query_movie()
    import time
    start = time.time()
    query_online("[变形金刚5：最后的骑士].Transformers.The.Last.Knight.2017.3D.BluRay.1080p.HSBS.x264.TrueHD7.1-CMCT")
    end = time.time()
    spend = end - start
    print(spend)