#!/bin/bash
queuename=`awk -F '=' '/\[HIVE\]/{a=1}a==1&&$1~/job.queuename/{print $2;exit}' ../../Data_Processing/Preprocessing/config.ini`
pt=$3

spark-submit --queue malg --master yarn --driver-memory 30G --num-executors 10 --executor-cores 8 --executor-memory 10G --conf "spark.excutor.memoryOverhead=15G" Feature_Engineering.py $pt
