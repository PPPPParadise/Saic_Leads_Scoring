#!/bin/bash
/usr/bin/spark-submit \
--master yarn \
--queue malg \
--driver-memory 30G \
--num-executors 5 \
--executor-cores 6 \
--executor-memory 10G \
--conf "spark.excutor.memoryOverhead=15G" \
--conf spark.driver.extraJavaOptions=" -Dfile.encoding=utf-8 " \
Fail_Reason.py $3