#!/bin/bash
pt=20190901
beginTime='2019-08-01 00:00:00'
echo "---------------------fail_feature_cleansing.sql start_1......" 
hive -hivevar pt=$pt -hivevar beginTime=$beginTime -f fail_feature_cleansing.sql >& fail_feature_cleansing.log
#$ if [ $? -ne 0 ];then    echo exit; else    echo "ok"; fi
echo "---------------------fail_feature_cleansing.sql end ......"
echo "---------------------fail_feature_processing.sql start._2....."
hive -hivevar queuename=$queuename -f fail_feature_processing.sql >& fail_feature_processing.log
echo "---------------------fail_feature_processing.sql end ......"
