from utils import UserDataApp


def get_inverted_table(cate_id):

    spark = UserDataApp().spark
    # topic_weights = spark.sql("select * from movie_portrait.topic_weights")\
    #             .where('cate_id = {}'.format(cate_id))
    topic_weights = spark.sql("select * from movie_portrait.topic_weights_normal")\
                .where('cate_id = {}'.format(cate_id))
    topic_weights = topic_weights.toPandas()
    # print(topic_weights.index)
    # print(topic_weights.values[0][0])
    # print(topic_weights.index.size)
    # print(topic_weights.shape[0], topic_weights.shape[1])
    # print(len(topic_weights))

    inverted_dict = {}
    # for mid, weights in movie_profile["weights"].iteritems():
    # i = 0
    for i in range(len(topic_weights)):
        topic = topic_weights['topic'][i]
        movie_id = topic_weights['movie_id'][i]
        weight = topic_weights['weight'][i]
        _ = inverted_dict.get(topic, [])
        if movie_id is not None:
            _.append((movie_id, weight))
        # else:
        #     i += 1
        #     print('{}'.format(i) * 20)
        inverted_dict.setdefault(topic, _)
    inverted_list = []
    for key, value in inverted_dict.items():
        inverted_list.append((key, str(sorted(value, key=lambda x: x[1], reverse=True)), len(value)))

    # sc = spark.sparkContext
    # rdd = sc.parallelize(inverted_list)
    # tmp = rdd.map(lambda x: Row(topic=x[0], movie_li=x[1], movie_count=x[2]))
    # inverted_table = spark.createDataFrame(tmp)
    from pyspark.sql import functions
    # inverted_table = inverted_table.withColumn('cate_id', functions.lit(cate_id))

    inverted_table = spark.createDataFrame(inverted_list, ['topic', 'movie_li', 'movie_count'])
    inverted_table = inverted_table.withColumn('cate_id', functions.lit(cate_id))

    return inverted_table



if __name__ == '__main__':
    tmp = get_inverted_table(1969)
    tmp.show(truncate=False)
    # print(tmp.count())