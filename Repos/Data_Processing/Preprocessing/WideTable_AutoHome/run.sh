#!/bin/bash
spark-submit --queue malg --master yarn --driver-memory 10G --num-executors 10 --executor-cores 6 --executor-memory 10G --conf "spark.excutor.memoryOverhead=10G" autohome.py $3