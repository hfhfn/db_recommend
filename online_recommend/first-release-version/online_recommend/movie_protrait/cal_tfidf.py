from pyspark.ml.feature import CountVectorizer
from utils import MovieDataApp, movie_protrait_db, movie_model_path
from pyspark.ml.feature import IDF
from functools import partial


spark = MovieDataApp().spark

def get_cv_idf_model():
    # doc = get_words()
    doc = spark.sql('select * from {}.cut_words'.format(movie_protrait_db))

    # count值计算
    # 这里将所有出现过的词都统计出来，这里最多会有n * 20个词
    cv = CountVectorizer(inputCol="words", outputCol="rawFeatures", vocabSize=270000 * 20, minDF=1.0)
    cv_model = cv.fit(doc)
    # 保存，加载模型
    cv_model.write().overwrite().save("{}CV.model".format(movie_model_path))
    # print("数据集中的词：", cv_model.vocabulary)   # vocabulary： 377958
    cv_result = cv_model.transform(doc)
    # cv_result.show(truncate=False)

    # IDF值计算
    idf = IDF(inputCol="rawFeatures", outputCol="features")
    idfModel = idf.fit(cv_result)
    # 保存，加载模型
    idfModel.write().overwrite().save("{}IDF.model".format(movie_model_path))


def get_tfidf():
    """
    计算tfidf
    :return: 【dataframe】
    """
    doc = spark.sql('select * from {}.cut_words'.format(movie_protrait_db))

    from pyspark.ml.feature import IDFModel
    idfModel = IDFModel.load("{}IDF.model".format(movie_model_path))
    from pyspark.ml.feature import CountVectorizerModel
    cv_model = CountVectorizerModel.load("{}CV.model".format(movie_model_path))

    cv_result = cv_model.transform(doc)
    rescaledData = idfModel.transform(cv_result)
    # rescaledData.select("words", "features").show()

    # 利用idf属性，获取每一个词的idf值，这里每一个值与cv_model.vocabulary中的词一一对应
    # print(idfModel.idf.toArray())
    keywords_list_with_idf = list(zip(cv_model.vocabulary, idfModel.idf.toArray()))
    # print(keywords_list_with_idf)

    # row = rescaledData.first()
    # print(row)
    # row.rawFeatures是一个向量类型
    # print(row.rawFeatures.indices)
    # print(row.rawFeatures.values)
    # print(row.rawFeatures[24])

    def _tfidf(partition, kw_list):
        """
        :param partition:
        :param kw_list: 【list[(),..]】  idf 词典
        :return:
        """
        for row in partition:
            # words_length = len(row.words)  # 统计文档中单词总数
            indices_li = row.features.indices
            # values_li = row.features.values  # 加values成列表了，index成了values_list的索引了，而不是词典的索引了
            values_li = row.features

            for index in indices_li:
                index = int(index)
                word, idf = kw_list[index]
                tfidf = values_li[index]
                # tf = row.rawFeatures[int(index)] / words_length  # 计算TF值
                # tfidf = float(tf) * float(idf)  # 计算该词的TFIDF值
                yield row.movie_id, row.cate_id, row.title, word, index, float(idf), round(float(tfidf), 4)

    # 使用partial为函数预定义要传入的参数
    tfidf = partial(_tfidf, kw_list=keywords_list_with_idf)
    # keyword_tfidf = cv_result.rdd.mapPartitions(tfidf)
    keyword_tfidf = rescaledData.rdd.mapPartitions(tfidf)
    keyword_tfidf = keyword_tfidf.toDF(["movie_id", "cate_id", "title", "keyword", "index", "idf", "tfidf"])
    # keyword_tfidf.show()
    # keyword_tfidf.orderBy("tfidf", ascending=False).show()

    return keyword_tfidf

def _get_topK_tfidf():    # 弃用， 参考
    TOPK = 20
    tfidf_ret = spark.sql(
        "select movie_id, collect_list(keyword) keywords, collect_list(tfidf) tfidf from protrait.tfidf group by movie_id")
    # tfidf_ret = get_tfidf()
    def sorted_tfidf(partition):
        for row in partition:
            # 找到索引与IDF值并进行排序
            _ = list(zip(row.keywords, row.tfidf))
            _ = sorted(_, key=lambda x: x[1], reverse=True)
            result = _[:TOPK]
            for keyword, tfidf in result:
                yield row.movie_id, keyword, tfidf

    # 使用partial为函数预定义要传入的参数
    result = tfidf_ret.rdd.mapPartitions(sorted_tfidf)
    result = result.toDF(["movie_id", "keyword", "tfidf"])
    # result.show()
    return result

    # 分组排序， 参考
    # doc = spark.sql("select * from protrait.tfidf")
    # 分组排序并取前20  movie_id 乱序
    # ret = doc.toPandas().sort_values(by='tfidf', ascending=False).groupby('movie_id').head(20)
    # 分组排序并取前20 （分组排序+分组切片）
    # ret = doc.toPandas().groupby('movie_id', group_keys=False).apply(lambda x: x.sort_values('tfidf', ascending=False)).groupby('movie_id').head(20)
    # print(ret.head(50))

def get_topK_tfidf(topK):
    """
    返回 topK个 tfidf结果
    :return: 【dataframe】
    """

    sql ="""SELECT movie_id, keyword, tfidf
    FROM    ( SELECT    ROW_NUMBER() OVER ( PARTITION  BY t1.movie_id ORDER BY t1.tfidf DESC) AS RNUM ,
                        *
              FROM      {}.tfidf t1
            ) AS T
    WHERE   T.RNUM <= {}""".format(movie_protrait_db, topK)

    result = spark.sql(sql)
    # result.show()
    return result

if __name__ == '__main__':
    # get_cv_idf_model()
    # get_tfidf()
    # get_topK_tfidf()
    pass