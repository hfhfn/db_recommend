import sys
from pyspark import SparkConf
from pyspark.sql import SparkSession
import os


sys.path.append("/home/local/spark-2.3.4/python")
sys.path.append("/home/local/spark-2.3.4/python/lib/py4j-0.10.7-src.zip")


class SparkSessionBase(object):
    # 配置spark driver和pyspark运行时，所使用的python解释器路径
    # PYSPARK_PYTHON = "/usr/bin/python3"
    PYSPARK_PYTHON = "/home/hadoop/miniconda3/envs/recommend/bin/python3"
    # 当存在多个版本时，不指定很可能会导致出错
    os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON
    os.environ["PYSPARK_DRIVER_PYTHON"] = PYSPARK_PYTHON

    SPARK_APP_NAME = None
    # SPARK_URL = "yarn"
    SPARK_URL = "local[*]"
    # SPARK_URL = "spark://hadoop-master:7077"
    JAVA_HOME = "/home/local/jdk/"
    # JAVA_HOME = "/home/hadoop/app/jdk1.8.0_211/"
    os.environ['JAVA_HOME'] = JAVA_HOME

    HADOOP_CONF_DIR = "/home/local/hadoop-2.9.2/etc/hadoop/"
    os.environ['HADOOP_CONF_DIR'] = HADOOP_CONF_DIR

    SPARK_EXECUTOR_MEMORY = "28g"
    SPARK_EXECUTOR_CORES = 4
    SPARK_EXECUTOR_INSTANCES = 4
    # JAVA_OPTS = "-server -Xms16g -Xmx16g -XX:PermSize=1g -XX:MaxNewSize=4g -XX:MaxPermSize=2g -Djava.awt.headless=true"
    # JAVA_OPTS = "-XX:+PrintGCDetails -XX:+PrintGCTimeStamps"
    ENABLE_HIVE_SUPPORT = False

    def _create_spark_session(self):

        conf = SparkConf()  # 创建spark config对象

        config = (
            ("spark.app.name", self.SPARK_APP_NAME),  # 设置启动的spark的app名称，没有提供，将随机产生一个名称
            ("spark.executor.memory", self.SPARK_EXECUTOR_MEMORY),  # 设置该app启动时占用的内存用量，默认2g
            ("spark.master", self.SPARK_URL),  # spark master的地址
            ("spark.executor.cores", self.SPARK_EXECUTOR_CORES),  # 设置spark executor使用的CPU核心数，默认是1核心
            ("spark.executor.instances", self.SPARK_EXECUTOR_INSTANCES),
            ("hive.metastore.uris", "thrift://hadoop-master:9083"),  # 配置hive元数据的访问，否则spark无法获取hive中已存储的数据
            # ("hive.metastore.uris", "thrift://hadoop:9083")  # 配置hive元数据的访问，否则spark无法获取hive中已存储的数据
            # ("spark.driver.maxResultSize", "16g"),
            # ("spark.sql.execution.arrow.enabled", "true"),
            # ("spark.driver.memory", "16g")
            # ("spark.sql.autoBroadcastJoinThreshold", 1024 * 1024 * 1024),    # 设定广播表大小的max值为1G
            ("spark.sessionState.conf.numShufflePartitions", 500),
            # ("spark.yarn.jars", "hdfs://hadoop-master:8020/tmp/spark_jars/*")
            # ("--py-files", "/home/online_recommend.zip")
            ("spark.kryoserializer.buffer.max", "1536m")  # 1.5 G
            )
        # pyFiles = ["/home/online_recommend.zip"]
        #.addFile("/home/online_recommend.zip")
        #.addPyFile("/home/online_recommend.zip")
        conf.setAll(config)

        # 利用config对象，创建spark session
        if self.ENABLE_HIVE_SUPPORT:
            return SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()
        else:
            return SparkSession.builder.config(conf=conf).getOrCreate()


class MovieDataApp(SparkSessionBase):
    SPARK_APP_NAME = "movie_data"

    ENABLE_HIVE_SUPPORT = True

    def __init__(self):
        self.spark = self._create_spark_session()
        # self.sc = self.spark.sparkContext
        # self.sc.addPyFile("/home/online_recommend.zip")


class UserDataApp(SparkSessionBase):
    SPARK_APP_NAME = "user_data"

    ENABLE_HIVE_SUPPORT = True

    def __init__(self):
        self.spark = self._create_spark_session()

class UpdateMovieApp(SparkSessionBase):
    """
    增量更新电影画像/召回
    """
    SPARK_APP_NAME = "updateMovie"
    ENABLE_HIVE_SUPPORT = True

    def __init__(self):
        self.spark = self._create_spark_session()

class UpdateUserApp(SparkSessionBase):
    """
    增量更新用户画像/召回
    """
    SPARK_APP_NAME = "updateUser"
    ENABLE_HIVE_SUPPORT = True

    def __init__(self):
        self.spark = self._create_spark_session()



if __name__ == '__main__':
    pass



# import os
#
# # PYSPARK_PYTHON = "/usr/bin/python3"
# JAVA_HOME = "/home/local/jdk/"
# # os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON
# # os.environ["PYSPARK_DIRVER_PYTHON"] = PYSPARK_PYTHON
# os.environ['JAVA_HOME'] = JAVA_HOME
#
# # spark配置信息
# # 注意：添加
# from pyspark import SparkConf
# from pyspark.sql import SparkSession
#
# SPARK_APP_NAME = "transData"
# # SPARK_URL = "spark://192.168.18.173:7077"
# # SPARK_URL = "yarn"
#
# conf = SparkConf()  # 创建spark config对象
# # conf = SparkConf().setAppName("AlertAPP").setMaster("local[*]").set("spark.ui.port", "4044")
# config = (
#     ("spark.app.name", SPARK_APP_NAME),  # 设置启动的spark的app名称，没有提供，将随机产生一个名称
#     ("spark.executor.memory", "1g"),  # 设置该app启动时占用的内存用量，默认1g
#     # ("spark.master", SPARK_URL),  # spark master的地址
#     ("spark.executor.cores", "1"),  # 设置spark executor使用的CPU核心数
#     ("hive.metastore.uris", "thrift://hadoop-master:9083"),  # 配置hive元数据的访问，否则spark无法获取hive中已存储的数据
#     # ("spark.ui.port", "4044")  # 更改worker端口
# )
#
# conf.setAll(config)
#
# # 利用config对象，创建spark session
# spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()
