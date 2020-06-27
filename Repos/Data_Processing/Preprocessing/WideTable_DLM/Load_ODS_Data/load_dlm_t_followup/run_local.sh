#!/bin/bash
pt=20190901
beginTime='2019-08-01 00:00:00'
echo "---------------------followup_feature_cleansing.sql start_1......" 
hive -hivevar pt=$pt -hivevar beginTime=$beginTime -f followup_feature_cleansing.sql >& followup_feature_cleansing.log
#$ if [ $? -ne 0 ];then    echo exit; else    echo "ok"; fi
echo "---------------------followup_feature_cleansing.sql end ......"
echo "---------------------followup_feature_processing.sql start._2....."
hive -hivevar queuename=$queuename -f followup_feature_processing.sql >& followup_feature_processing.log
echo "---------------------followup_feature_processing.sql end ......"
