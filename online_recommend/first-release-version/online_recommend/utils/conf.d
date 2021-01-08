[program:recommend]  ;工程名称

environment=JAVA_HOME=/home/local/jdk,SCALA_HOME=/home/local/scala-2.11.8,SPARK_HOME=/home/local/spark-2.3.4,SPARK_CONF_DIR=/home/local/spark-2.3.4/conf,HADOOP_HOME=/home/local/hadoop-2.9.2,HIVE_HOME=/home/local/hive-2.3.6,HIVE_CONF_DIR=/home/local/hive-2.3.6/conf,PYSPARK_PYTHON=/home/hadoop/miniconda3/envs/recommend/bin/python,PYSPARK_DRIVER_PYTHON=/home/hadoop/miniconda3/envs/recommend/bin/python

command=/home/online_recommend/run.sh  ;执行的命令（脚本）

stdout_logfile=/home/online_recommend/logs/supervisor_out.log ;log的位置

stderr_logfile=/home/online_recommend/logs/supervisor_error.log  ;错误log的位置

directory=/home/online_recommend  ;路径

autostart=true  ;是否自动启动

autorestart=true  ;是否自动重启

startretries=10 ;失败的最大尝试次数

user=hadoop

;redirect_stderr=true

loglevel=info

stopsignal=KILL

stopasgroup=true

killasgroup=true
