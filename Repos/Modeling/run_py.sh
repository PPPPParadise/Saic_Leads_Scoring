#!/bin/bash

spark-submit --master yarn --driver-memory 30G --num-executors 10 --executor-cores 6 --executor-memory 30G --conf "spark.excutor.memoryOverhead=15G"  --conf spark.pyspark.driver.python=/usr/bin/python2.7  Model_Execution.py