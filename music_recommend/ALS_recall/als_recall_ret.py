from utils import u_spark, update_user_db, movie_original_db, factor_db, RetToHive, als_db, get_latest_recall, \
                user_portrait_db, user_pre_db


class ALSRecall(object):

    def get_negative_rating_matrix(self):
        # user_movie_rating = u_spark.sql(
        #     "select user_id, cate_id, collect_list(movie_id) positive, collect_list(weight) weight, \
        #     count(movie_id) sample_count from {}.action_weight group by user_id, cate_id".format(user_portrait_db))

        user_movie_rating = u_spark.sql(
            "select user_id, cate_id, collect_list(movie_id) positive, count(movie_id) sample_count \
            from {}.merge_click group by user_id, cate_id".format(user_pre_db))
        # user_movie_rating.show()
        # 使用hot_movie作为负样本，采集负样本池时以下面用户最大行为数为基础
        hot_movie = u_spark.sql("select cid cate_id, collect_list(aid) negative from {}.full_movie_hot_sort \
                        where weight > 100 group by cid".format(factor_db))

        user_movie_rating = user_movie_rating.join(hot_movie, on='cate_id', how='left').dropna()
        RetToHive(u_spark, user_movie_rating, als_db, 'pre_user_movie_rating')
        import gc
        del hot_movie
        gc.collect()
        """
        每个频道下，看电影最多的用户的行为电影数
        cid     max（count）
        0	    3579
        1972	382
        2272	127
        1969	2614
        1973	925
        1970	671
        1974	646
        1971	304
        2271	132
        7973	14

        # cate_li = [0, 1972, 2272, 1969, 1973, 1970, 1974, 1971, 2271]
        # # get_num = [3579, 382, 127, 2614, 925, 671, 646, 304, 132]
        # num_li = [4000, 500, 200, 3000, 1200, 1000, 1000, 500, 200]    # 适当放大负样本池
        # hot_movie = u_spark.sql("select * from {}.movie_hot_sort".format(factor_db))
        # sample_hot_movie = None
        # for i, num in enumerate(num_li):
        #     hot_cate_movie = hot_movie.where("cate_id = {} and sort_num <= {}".format(cate_li[i], num))
        #     if i == 0:
        #         sample_hot_movie = hot_cate_movie
        #     else:
        #         sample_hot_movie = sample_hot_movie.union(hot_cate_movie)
        """

        # user_movie_rating = u_spark.sql("select * from {}.pre_user_movie_rating".format(als_db)).dropna()
        # 直接把整个hot_sort作为负样本池，因为有排序，所以可以选择前面热度高的作为负样本，以1:1的比例采样
        # def add_negative_sample(row):
        #     # # 虽然此方法不改变原列表顺序，但时间复杂度很高，所以用下面那种方法替代
        #     # _negative = [i for i in row.negative if i not in row.positive]
        #
        #     # 因为set会改变原列表顺序，先以1：2的比例选取负样本池，再从其中随机采样
        #     _negative = row.negative[:row.sample_count * 2 + 1]
        #     # 去除重复的movie_id, 再采样
        #     _negative = list(set(_negative).difference(set(row.positive)))
        #     # 正负样本 1：1 采样
        #     _negative = _negative[:row.sample_count + 1]
        #     # 负样本权重统一设置为0.0 （浮点数，为了保持和正样本权重类型一致） 以便hive存储
        #     negative = dict(zip(_negative, [0.0] * row.sample_count))
        #     # print('negative: ', negative)
        #     positive = dict(zip(row.positive, row.weight))
        #     movie_li = positive
        #     movie_li.update(negative)
        #     # print('movie_li: ', movie_li)
        #     return row.user_id, row.cate_id, movie_li

        def add_negative_sample(row):
            # # 因为set会改变原列表顺序，先以1：2的比例选取负样本池，再从其中随机采样
            # _negative = row.negative[:row.sample_count * 2 + 1]
            # # 去除重复的movie_id, 再采样, 此方法也很慢
            # _negative = list(set(_negative).difference(set(row.positive)))

            # 循环遍历删除的方法不改变原列表顺序，但也太慢
            positive = set(row.positive)
            _negative = row.negative
            for i in positive:
                try:
                    _negative.remove(i)
                except:
                    continue
            negative_num = len(_negative)

            # 虽然此方法不改变原列表顺序，但时间复杂度很高 （最差方法弃用）
            # _negative = [i for i in row.negative if i not in set(row.positive)]
            # 正负样本 1：1 采样
            negative = []
            if negative_num == 0:
                negative = {}
            else:
                import random
                for i in range(row.sample_count):
                    negative.append(_negative[random.randint(0, negative_num-1)])
                negative = dict(zip(negative, [0] * row.sample_count))

            # print('negative: ', negative)
            positive = dict(zip(row.positive, [1] * row.sample_count))
            movie_li = positive
            movie_li.update(negative)
            # print('movie_li: ', movie_li)
            return row.user_id, row.cate_id, movie_li

        # from pyspark.shell import sqlContext
        # user_movie_rating = sqlContext.createDataFrame(user_movie_rating,
        #                                                ["user_id", "cate_id", "movie_li"], samplingRatio=0.2)
        user_movie_rating = user_movie_rating.rdd.map(add_negative_sample).toDF(["user_id", "cate_id", "movie_li"])
        RetToHive(u_spark, user_movie_rating, als_db, 'map_user_movie_rating')
        # user_movie_rating.show()
        user_movie_rating.registerTempTable("tempTable")
        user_movie_rating = u_spark.sql("select user_id, cate_id, movie_id, weight \
                                            from tempTable LATERAL VIEW explode(movie_li) AS movie_id, weight")
        RetToHive(u_spark, user_movie_rating, als_db, 'user_movie_rating')

        # user_movie_rating = u_spark.sql("select * from {}.user_movie_rating".format(als_db))
        from pyspark.ml.feature import StringIndexer
        from pyspark.ml import Pipeline
        # 用户和电影ID超过ALS最大整数值，需要使用StringIndexer进行转换
        user_id_indexer = StringIndexer(inputCol='user_id', outputCol='als_user_id')
        movie_id_indexer = StringIndexer(inputCol='movie_id', outputCol='als_movie_id')
        pip = Pipeline(stages=[user_id_indexer, movie_id_indexer])
        pip_fit = pip.fit(user_movie_rating)
        als_user_movie_rating = pip_fit.transform(user_movie_rating)

        RetToHive(u_spark, als_user_movie_rating, als_db, 'als_negative_rating')

    def get_rating_matrix(self):
        # user_movie_rating = u_spark.sql(
        #     "select user_id, cate_id, movie_id, weight from {}.action_weight".format(user_portrait_db))
        import pyspark.sql.functions as F
        user_movie_rating = u_spark.sql(
                    "select user_id, cate_id, movie_id from {}.merge_click".format(user_pre_db))\
                    .withColumn('weight', F.lit(1))

        from pyspark.ml.feature import StringIndexer
        from pyspark.ml import Pipeline
        # 用户和电影ID超过ALS最大整数值，需要使用StringIndexer进行转换
        user_id_indexer = StringIndexer(inputCol='user_id', outputCol='als_user_id')
        movie_id_indexer = StringIndexer(inputCol='movie_id', outputCol='als_movie_id')
        pip = Pipeline(stages=[user_id_indexer, movie_id_indexer])
        pip_fit = pip.fit(user_movie_rating)
        als_user_movie_rating = pip_fit.transform(user_movie_rating)

        RetToHive(u_spark, als_user_movie_rating, als_db, 'als_user_movie_rating')

    def eval_als(self, cate_id, method, negative, implicit):
        """
        :param cate_id:
        :param method:    regression, classification
        :param implicit:
        :return:
        """
        from pyspark.ml.recommendation import ALS
        if negative:
            als_user_movie_rating = u_spark.sql('select * from {}.als_negative_rating where cate_id = {}'
                                                .format(als_db, cate_id))
        else:
            als_user_movie_rating = u_spark.sql('select * from {}.als_user_movie_rating where cate_id = {}'
                                                .format(als_db, cate_id))

        # 可以定性为 回归问题、二分类问题 分别构建 label 或 weight
        # if method != 'regression':
        # if method == 'classification':
        #     def map(row):
        #             # try:
        #             if row.weight > 0:
        #                 label = 1
        #             else:
        #                 label = 0
        #             return row.user_id, row.movie_id, row.als_user_id, row.als_movie_id, label
        #
        #     als_user_movie_rating = als_user_movie_rating.rdd.map(map).toDF(
        #                 ['user_id', 'movie_id', 'als_user_id', 'als_movie_id', 'label'])
        """
        # if method == 'bi_class':   # binary classification
        #     def map(row):
        #         # try:
        #         if row.weight > 0:
        #             label = 1
        #         else:
        #             label = 0
        #         return row.user_id, row.movie_id, row.als_user_id, row.als_movie_id, label
        # elif method == 'multi_class':   # multi_classification
        #     import pyspark.sql.functions as fn
        #     tmp = als_user_movie_rating.agg(fn.max('weight').alias('max')).first()
        #     weight_max = tmp.max
        #     def map(row):
        #         # try:
        #         weight = row.weight
        #         if weight == 0:
        #             label = 0
        #         elif 0 < weight <= weight_max / 3:
        #             label = 1
        #         elif weight_max / 3 < weight <= weight_max * 2 / 3:
        #             label = 2
        #         else:
        #             label = 3
        #         return row.user_id, row.movie_id, row.als_user_id, row.als_movie_id, label
        """

        # 拆分数据集，得到训练和评估数据集
        trainDF, testDF = als_user_movie_rating.randomSplit([0.8, 0.2], seed=10)
        # _, testDF = u_spark.sql('select * from {}.als_negative_rating where cate_id = {}'
        #                                         .format(als_db, cate_id)).randomSplit([0.8, 0.2], seed=10)

        # 网格搜索，遍历最优参数组合
        evalData = []
        if implicit:
            for ranks in [50]:    #  rank 模型中隐藏因子数目 即矩阵隐因子数，默认为10
                for maxIters in [10]:    # maxIter 算法迭代次数  默认 10
                    # regParams 0.001, 0.01, 0.1, 1
                    for regParams in [0.1]:   # regParam 正则项权重 默认 0.1
                        # alphas 0.1, 1, 10, 20, 30
                        for alphas in [30]:  # alpha 用户对物品偏好的可信度,应用于隐式反馈
                            # Nonnegative: 商品推荐分数是否是非负的
                            # implicitPrefs: 数据集是隐式反馈还是显示反馈  False为显示，True为隐式
                            als = ALS(rank=ranks, maxIter=maxIters, regParam=regParams, alpha=alphas, seed= 10,
                                      implicitPrefs=True, userCol='als_user_id', itemCol='als_movie_id',
                                      ratingCol='weight', nonnegative=True, checkpointInterval=1)
                            # 模型训练和推荐每个用户固定影视个数
                            model = als.fit(trainDF)
                            testPredic = model.transform(testDF)
                            # testPredic2 = model.transform(trainDF)
                            # testPredic2.show()

                            testPredicData = testPredic.fillna(0).rdd\
                            .map(lambda row: (row.user_id, row.movie_id, row.weight, round(row.prediction)))\
                                .toDF(['user_id', 'movie_id', 'weight', 'prediction'])
                            testPredicData = testPredicData.select('user_id', 'movie_id', 'weight',
                                         testPredicData.prediction.cast("Double"))
                            # RetToHive(u_spark, testPredicData, als_db, 'testPredicData')
                            testPredicData.where('weight=0').show(50)
                            from pyspark.ml.evaluation import BinaryClassificationEvaluator
                            evaluator = BinaryClassificationEvaluator(rawPredictionCol='prediction', labelCol='weight')
                            auc = evaluator.evaluate(testPredicData, {evaluator.metricName: "areaUnderROC"})
                            evalData.append([ranks, maxIters, regParams, alphas, auc])
                            print(ranks, maxIters, regParams, alphas, 'auc: ', auc)

                            """
                            ALS模型，不能进行多分类预测
                            """
                            # testPredicData = testPredic.fillna(0)\
                            #     .rdd.map(lambda row: (row.user_id, row.movie_id, row.label, round(row.prediction)))\
                            #     .toDF(['user_id', 'movie_id', 'label', 'prediction'])
                            # testPredicData = testPredicData.select('user_id', 'movie_id', 'label',
                            #                                    testPredicData.prediction.cast("Double"))
                            # testPredicData.where('label = 2').show(100)
                            # if method == 'multi_class':
                            #     from pyspark.ml.evaluation import MulticlassClassificationEvaluator
                            #     evaluator = MulticlassClassificationEvaluator(predictionCol='prediction', labelCol='label',
                            #                                                   metricName='f1')
                            #     f1 = evaluator.evaluate(testPredicData)
                            #     evalData.append([ranks, maxIters, regParams, alphas, f1])
                            #     print(ranks, maxIters, regParams, alphas, 'f1: ', f1)
                            # if method == 'bi_class':
                            #     from pyspark.ml.evaluation import BinaryClassificationEvaluator
                            #     evaluator = BinaryClassificationEvaluator(rawPredictionCol='prediction', labelCol='label',
                            #                                               metricName='areaUnderROC')
                            #     auc = evaluator.evaluate(testPredicData)
                            #     evalData.append([ranks, maxIters, regParams, alphas, auc])
                            #     print(ranks, maxIters, regParams, alphas, 'auc: ', auc)

        else:
            for ranks in [50]:  # rank 模型中隐藏因子数目 即矩阵隐因子数，默认为10
                for maxIters in [10]:  # maxIter 算法迭代次数  默认 10
                    # regParams 0.001, 0.01, 0.1, 1
                    for regParams in [0.1]:  # regParam 正则项权重 默认 0.1
                        # Nonnegative: 商品推荐分数是否是非负的
                        # implicitPrefs: 数据集是隐式反馈还是显示反馈
                        if method == 'regression':
                            als = ALS(rank=ranks, maxIter=maxIters, regParam=regParams, seed=10,
                                      implicitPrefs=True, userCol='als_user_id', itemCol='als_movie_id',
                                      ratingCol='weight', nonnegative=True, checkpointInterval=1)
                            # 模型训练和推荐每个用户固定影视个数
                            model = als.fit(trainDF)
                            testPredic = model.transform(testDF)
                            # testPredic = model.transform(trainDF)

                            # testPredic.show()
                            testPredicData = testPredic.select('user_id', 'movie_id', 'weight',
                                                               testPredic.prediction.cast("Double")).fillna(0)
                            from pyspark.ml.evaluation import RegressionEvaluator
                            evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='weight')
                            loss = evaluator.evaluate(testPredicData, {evaluator.metricName: 'rmse'})  # 默认 rmse
                            evalData.append([ranks, maxIters, regParams, loss])
                            print(ranks, maxIters, regParams, loss)
                        else:
                            als = ALS(rank=ranks, maxIter=maxIters, regParam=regParams, seed=10,
                                      implicitPrefs=True, userCol='als_user_id', itemCol='als_movie_id',
                                      ratingCol='label', nonnegative=True, checkpointInterval=1)
                            # 模型训练和推荐每个用户固定影视个数
                            model = als.fit(trainDF)
                            testPredic = model.transform(testDF)
                            # testPredic = model.transform(trainDF)

                            testPredicData = testPredic.select('user_id', 'movie_id', 'label',
                                                               testPredic.prediction.cast("Double")).fillna(0)
                            from pyspark.ml.evaluation import BinaryClassificationEvaluator
                            evaluator = BinaryClassificationEvaluator(rawPredictionCol='prediction', labelCol='label',
                                                                      metricName='areaUnderROC')
                            roc = evaluator.evaluate(testPredicData)
                            evalData.append([ranks, maxIters, regParams, roc])
                            print(ranks, maxIters, regParams, roc)

    def train_als(self, cate_id):
        from pyspark.ml.recommendation import ALS
        # als_user_movie_rating = u_spark.sql('select * from {}.als_user_movie_rating where cate_id = {}'
        #                                     .format(als_db, cate_id))
        als_user_movie_rating = u_spark.sql('select * from {}.als_negative_rating where cate_id = {}'
                                            .format(als_db, cate_id))

        # 默认组合就还可以，尝试最佳组合：rank，maxIters，regParams [50, 10, 0.1], 但测试组损失相差都不大，预测结果都不理想
        als = ALS(rank=50, maxIter=10, regParam=0.01, seed=10,
                  implicitPrefs=False, userCol='als_user_id', itemCol='als_movie_id',
                  ratingCol='weight', nonnegative=True, checkpointInterval=1)
        # 模型训练和推荐每个用户固定影视个数
        model = als.fit(als_user_movie_rating)
        recall_res = model.recommendForAllUsers(500)  # .show(truncate=False)

        # recall_res得到需要使用StringIndexer变换后的下标
        # 保存原来的下表映射关系
        refection_user = als_user_movie_rating.groupBy(['user_id']).max('als_user_id').withColumnRenamed(
            'max(als_user_id)', 'als_user_id')
        refection_movie = als_user_movie_rating.groupBy(['movie_id']).max('als_movie_id').withColumnRenamed(
            'max(als_movie_id)', 'als_movie_id')
        recall_res = recall_res.join(refection_user, on=['als_user_id'], how='left').select(
            ['als_user_id', 'recommendations', 'user_id'])
        import pyspark.sql.functions as F
        recall_res = recall_res.withColumn('als_movie_id_weight', F.explode('recommendations')).drop('recommendations')

        def _movie_id(row):
            return row.als_user_id, row.user_id, row.als_movie_id_weight[0], row.als_movie_id_weight[1]

        # 获得推荐的电影的id
        als_recall = recall_res.rdd.map(_movie_id).toDF(['als_user_id', 'user_id', 'als_movie_id', 'weight'])
        als_recall = als_recall.join(refection_movie, on=['als_movie_id'], how='left').select(
            ['user_id', 'movie_id', 'weight'])
        # als_recall.show()
        movie = u_spark.sql("select id movie_id, title, year from {}.db_asset where cid = {}".format(
            movie_original_db, cate_id))
        als_recall = als_recall.join(movie, on='movie_id', how='left')
        # als_recall = als_recall.groupBy('user_id').agg(F.collect_list('movie_id')).withColumnRenamed(
        #   'collect_list(movie_id)', 'movie_list').dropna()
        RetToHive(u_spark, als_recall, als_db, 'als_recall_{}'.format(cate_id))

        return als_recall

    def get_filter_als_ret(self, cate_id):
        als_recall = u_spark.sql('select * from {}.als_recall_{}'.format(als_db, cate_id))\
                            .select('user_id', 'movie_id', 'weight')

        recall_df = get_latest_recall(als_recall, cate_id, als_db, filter='user')

        return recall_df


if __name__ == '__main__':
    # ALSRecall().get_negative_rating_matrix()
    # ALSRecall().get_rating_matrix()
    ALSRecall().eval_als(cate_id=1969, method='classification', negative=True, implicit=True)
    # ALSRecall().train_als(cate_id=1969).show()
    # ALSRecall().get_filter_als_ret(1969).show()
