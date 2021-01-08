#!/bin/bash

. /etc/profile
#echo `env | grep hive`
cd `$dirname`|exit 0   # 进入当前文件夹
#echo `pwd`
source activate recommend
#echo `whereis python`
# -u 参数 使命令输出 stdout不写缓存， 进而stderr和stdout直接输出到日志
python3 -u /home/online_recommend/update_scheduler/update.py > logs/recommend.out
