from utils import movie_recall_db, update_user_db, dim, k, u_topK, RetToHive, user_recall_db


def compute_user_similar(spark, cate_id):
    """
    计算增量用户向量与电影向量的相似度 word2vec
    从计算电影向量开始，分类型处理
    :return:
    """
    # 直接读取全量的 movie_vector 进行计算
    movie_vector = spark.sql("select * from {}.movie_vector_{}".format(movie_recall_db, cate_id))
    # spark.sql("use {}".format(update_user_db))
    # 读取 全量或者增量的 user_vector 进行计算
    user_vector = spark.sql("select * from {}.user_vector_{}".format(user_recall_db, cate_id))

    update_userVector_list = user_vector.toPandas().values[:, 2].tolist()
    update_userVector_id = user_vector.select('user_id').toPandas()
    import numpy as np
    update_userVector_array = np.asarray(update_userVector_list).astype('float32')

    movie_vector_list = movie_vector.toPandas().values[:, 2].tolist()
    movie_vector_array = np.asarray(movie_vector_list).astype('float32')

    import gc
    del user_vector
    del update_userVector_list
    del movie_vector_list
    gc.collect()

    import faiss
    # quantizer = faiss.IndexFlatL2(self.dim)  # L2距离，即欧式距离（越小越好）
    quantizer = faiss.IndexFlatIP(dim)  # 点乘，归一化的向量点乘即cosine相似度（越大越好）
    m = 4  # 把原始的向量空间分解为m个低维向量空间的笛卡尔积
    gzip = 8  # 每个向量压缩为8bit
    index = faiss.IndexIVFPQ(quantizer, dim, k, m, gzip)
    index.nprobe = 5  # 同时查找聚类中心的个数，默认为1个，若nprobe=nlist(self.k)则等同于精确查找
    # print(index.is_trained)
    index.train(movie_vector_array)  # 需要训练
    index.add(movie_vector_array)  # 添加训练时的样本
    # print(index.ntotal)
    D, I = index.search(update_userVector_array, u_topK)  # 寻找相似向量， I表示相似用户ID矩阵， D表示距离矩阵
    # print(D[:5])
    # print(I[-5:])

    import pandas as pd
    df = pd.DataFrame(columns=['_index'])
    df.append(pd.Series([None]), ignore_index=True)
    df['_index'] = I.tolist()
    df2 = pd.DataFrame(columns=['similar'])
    df2.append(pd.Series([None]), ignore_index=True)
    df2['similar'] = D.tolist()

    concat_df = pd.concat([update_userVector_id, df, df2], axis=1)
    sp_df = spark.createDataFrame(concat_df)

    movie_vector_id = movie_vector.select('movie_id').toPandas()

    def map(partition):
        for row in partition:
            for i in range(u_topK):
                movie_id = movie_vector_id.iloc[row._index[i], 0]
                yield row.user_id, int(movie_id), row.similar[i]

    recall_df = sp_df.rdd.mapPartitions(map).toDF(['user_id', 'movie_id', 'cos_sim'])

    del movie_vector
    del update_userVector_id
    del update_userVector_array
    del movie_vector_id
    del movie_vector_array
    del index
    del df
    del df2
    del concat_df
    del sp_df
    del D
    del I
    gc.collect()

    RetToHive(spark, recall_df, user_recall_db, 'user_similar_{}'.format(cate_id))
    # recall_df.write.insertInto('user_similar_{}'.format(self.cate_id), overwrite=True)
    return recall_df