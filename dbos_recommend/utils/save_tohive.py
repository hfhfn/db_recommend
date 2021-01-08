from utils import BASE_DIR


class RetToHive(object):
    def __init__(self, spark_app, ret_data, database_name, table_name):
        """
        :param spark_app: 【object】 spark实例
        :param ret_data:  【dataframe】  需要保存的数据处理结果
        :param database_name:  【str】  保存的数据库名
        :param table_name:  【str】  保存的表名
        """
        # 指定spark实例
        self.spark = spark_app

        # 指定保存的数据库名和表名
        self.database_name = database_name
        self.table_name = table_name
        self.table_path = "{}.{}".format(self.database_name, self.table_name)

        # self.database_name = 'protrait'
        # self.predata_table_name = 'movie_feature'
        # self.textrank_table_name = 'textrank'

        # 指定创库、创表语句
        self.creat_database()
        # self.predata_sql = ''
        # self.textrank_sql = ''
        self.create_table_sql = ''
        self.ret_data = ret_data
        self.table_sql()
        # 保存结果
        self.ret_tohive()

    def creat_database(self):
        # 由于当前开启了hive sql功能，因此这里先创建库

        # 创建数据库和表时， hive.sql 可以在创表语句末尾添加 location '/user/hive/warehouse/protrait.db'
        # 来定义hdfs中的位置和数据库/表名
        # spark.sql使用 location 命令时报错： pyspark.sql.utils.AnalysisException: \
        #     'org.apache.hadoop.hive.ql.metadata.HiveException: ' \
        #     'MetaException(message:Unable to create database path file:/user/hive/warehouse/test.db,
        #     failed to create database test);'
        # 可能原因： hive.site.xml中配置 datanucleus.schema.autoCreateAll 需要改为 true，
        # 然后需要删除MySQL中hive表重新初始化 metastore： 命令： schematool -dbType mysql -initSchema
        self.spark.sql("create database if not exists {} comment '{}' \
                        location '/user/hive/warehouse/{}.db'"
                       .format(self.database_name, self.database_name, self.database_name))

    def table_sql(self):
        # 将spark的dataframe注册为临时表，以便能对其使用sql语句
        self.ret_data.registerTempTable("tempTable")
        # 查看临时表结构
        tmp = self.spark.sql("desc tempTable")
        ret = tmp.select('col_name', 'data_type').toPandas().values
        str = ''
        for i in ret:
            str += i[0] + ' ' + i[1].upper() + ',\n'
        str = str[:-2]

        self.create_table_sql = '''
        CREATE TABLE IF NOT EXISTS {} (
            {}
        )
        '''.format(self.table_path, str)
        # 加 LOCATION 创建的表，删除hive表时，hadoop文件不关联删除
        # LOCATION '/user/hive/warehouse/{}.db/{}'
        # '''.format(self.table_path, str, self.database_name, self.table_name)

# self.predata_sql = '''
        # CREATE TABLE {}.{} (
        # id INT comment "movie_id",
        # cid INT comment "channel_id",
        # update_time BIGINT comment "update_time",
        # summary STRING comment "describe information",
        # row_number INT comment "row index"
        # )
        # COMMENT 'movie feature'
        # '''.format(self.database_name, self.predata_table_name)

        # self.textrank_sql = """CREATE TABLE IF NOT EXISTS {}.{}(
        # movie_id INT,
        # industry STRING,
        # tag STRING,
        # weights DOUBLE
        # )
        # COMMENT 'textrank'
        # """.format(self.database_name, self.textrank_table_name)
        # # hive.sql中末尾可添加:  LOCATION '/user/hive/warehouse/protrait.db/movie_feature'

    def ret_tohive(self):

        self.spark.sql("DROP TABLE IF EXISTS {}".format(self.table_path))
        self.spark.sql(self.create_table_sql)
        # 写入数据到hive表
        self.spark.sql("INSERT INTO {} SELECT * FROM tempTable".format(self.table_path))

        # 检查hive表中的数据
        # count = self.spark.sql("select * from {}".format(self.table_path)).count()
        # print("=" * 50 + self.table_path + "=" * 50)
        # print(count)
        # self.spark.sql("select * from {}".format(self.table_path)).show()

        # import os
        # from datetime import datetime
        # now = datetime.today().strftime('%Y-%m-%d')
        # data_count_path = os.path.join(BASE_DIR, "data/data_count/{}_{}.txt".format(self.database_name, now))
        # with open(data_count_path, 'a', encoding='utf-8') as f:
        #     f.write(self.table_name + '\t' + str(count) + '\n')


if __name__ == '__main__':
    pass





