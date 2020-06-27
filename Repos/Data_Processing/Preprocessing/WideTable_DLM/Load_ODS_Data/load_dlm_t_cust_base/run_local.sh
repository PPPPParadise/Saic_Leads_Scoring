#!/bin/bash
pt=20190901
beginTime="2020-05-01 00:00:00"
echo "---------------------  cust feature cleansing begin   -----------------------"
echo "---------------------step1/cust_feature_cleansing.sql start_1......" 
hive -hivevar pt=$pt -hivevar pt=$beginTime -f cust_feature_cleansing.sql >& cust_feature_cleansing.log
#$ if [ $? -ne 0 ];then    echo exit; else    echo "ok"; fi
echo "---------------------step1/cust_feature_cleansing.sql end ......"
echo "---------------------step1/cust_feature_processing.sql start._2....."
hive -hivevar queuename=$queuename -f cust_feature_processing.sql >& cust_feature_processing.log
echo "---------------------step1/cust_feature_processing.sql end ......"

echo "---------------------  cust feature cleansing end   -----------------------"