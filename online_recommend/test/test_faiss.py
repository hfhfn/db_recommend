import faiss
import numpy as np
from utils.spark_app import MovieDataApp
from utils.save_tohive import RetToHive

spark = MovieDataApp().spark

spark.sql('use movie_recall')
# id = spark.sql('select movie_id from movie_vector_1969').limit(10000)
user_vector_arr = spark.sql('select * from movie_vector_1969').limit(100).toPandas().values[:, 2].tolist()
user_vector_arr = np.asarray(user_vector_arr).astype('float32')
# print(type(user_vector_arr))
# print(user_vector_arr.shape)
# print(user_vector_arr)
# user_vector_arr.printSchema()
gds_vector_arr = spark.sql('select movievector from movie_vector_1969').limit(100).toPandas().values[:, 0].tolist()
gds_vector_arr = np.asarray(gds_vector_arr).astype('float32')
# print(gds_vector_arr.shape)
# print(gds_vector_arr)

# user_vector_arr  # shape(1000, 100)
# gds_vector_arr   # shape(100, 100)
dim = 100# 向量维度
k = 10  # 定义召回向量个数
index = faiss.IndexFlatL2(dim)  # L2距离，即欧式距离（越小越好）
# index=faiss.IndexFlatIP(dim) # 点乘，归一化的向量点乘即cosine相似度（越大越好）
# print(index.is_trained)
index.add(gds_vector_arr) # 添加训练时的样本
# print(index.ntotal)
D, I = index.search(user_vector_arr, k) # 寻找相似向量， I表示相似用户ID矩阵， D表示距离矩阵
print(D[:5])
print(I[-5:])

import pandas as pd
df = pd.DataFrame(columns=['index'])
df.append(pd.Series([None]), ignore_index=True)
df['index'] = I.tolist()
df2 = pd.DataFrame(columns=['similar'])
df2.append(pd.Series([None]), ignore_index=True)
df2['similar'] = D.tolist()  # 欧式距离

concat_df = pd.concat([df, df2], axis=1)
# print(concat_df)
sp_df = spark.createDataFrame(concat_df)
# sp_df.show()