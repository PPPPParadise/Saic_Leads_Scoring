#!/bin/bash
pt=$3
spark-submit --queue malg --master yarn --driver-memory 30G --num-executors 10 --executor-cores 8 --executor-memory 30G --conf "spark.excutor.memoryOverhead=15G"  Model_Execution.py $pt