#!/bin/bash
pt=20190901
beginTime='2019-08-01 00:00:00'
echo "---------------------hall_flow_feature_cleansing.sql start_1......" 
hive -hivevar pt=$pt -hivevar beginTime=$beginTime -f hall_flow_feature_cleansing.sql >& hall_flow_feature_cleansing.log
#$ if [ $? -ne 0 ];then    echo exit; else    echo "ok"; fi
echo "---------------------hall_flow_feature_cleansing.sql end ......"
echo "---------------------hall_flow_feature_processing.sql start._2....."
hive -hivevar queuename=$queuename -f hall_flow_feature_processing.sql >& hall_flow_feature_processing.log
echo "---------------------hall_flow_feature_processing.sql end ......"
