from utils import MovieDataApp, movie_portrait_db


def idf_textrank():
    """
    返回 idf * textrank 权重
    :return: 【dataframe】
    """
    spark = MovieDataApp().spark
    # textrank * idf
    sql = """
    select r.movie_id, r.cate_id, t.title, r.tag keyword, (r.textrank * t.idf) weights 
    from {}.textrank r join {}.desc_tfidf t 
    where r.movie_id=t.movie_id and r.tag=t.keyword
    """.format(movie_portrait_db, movie_portrait_db)
    rt = spark.sql(sql) # .dropDuplicates(['movie_id', 'cate_id', 'keywords'])
    # print(rt.count())   # 4961767
    # rt.show()
    return rt

def tfidf_textrank():   # 暂时未用
    """
    返回 (tfidf + textrank) / 2 权重
    :return: 【dataframe】
    """
    spark = MovieDataApp().spark
    # (tfidf + textrank) / 2
    sql = """
    select r.movie_id, r.cate_id, t.title, r.tag keyword, (r.textrank + t.tfidf)/2 weights 
    from {}.textrank r join {}.desc_tfidf t 
    where r.movie_id=t.movie_id and r.tag=t.keyword
    """.format(movie_portrait_db, movie_portrait_db)
    # and
    # (r.weights + t.tfidf)/2 < 100
    rt = spark.sql(sql)
    # rt.orderBy("weights", ascending=False).show()
    return rt

def get_topK_weight(merge_weight, topK):
    """
    :param merge_weight: 【dataframe】 传入混合权重结果
    :return: 【dataframe】
    """
    spark = MovieDataApp().spark
    ret = merge_weight
    ret.registerTempTable("temptable")
    ret = spark.sql(
        "select movie_id, min(cate_id) cate_id, title, collect_list(keyword) keywords, collect_list(weights) weights \
        from temptable group by movie_id, title")
    def sorted_weight(partition):
        for row in partition:
            _ = list(zip(row.keywords, row.weights))
            _ = sorted(_, key=lambda x: x[1], reverse=True)
            result = _[:topK]
            for keyword, weight in result:
                yield row.movie_id, row.cate_id, row.title, keyword, weight

    # 使用partial为函数预定义要传入的参数
    result = ret.rdd.mapPartitions(sorted_weight)
    result = result.toDF(["movie_id", "cate_id", "title", "keyword", "weight"])
    # result.show()
    return result


if __name__ == '__main__':
    # tfidf_textrank()
    # get_topK_weight(idf_textrank()).show()
    idf_textrank()