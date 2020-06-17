#!/bin/bash
pt=20190901
echo "---------------------  cust feature cleansing begin   -----------------------"
echo "---------------------step1/cust_feature_cleansing.sql start_1......" 
hive -hivevar pt=$pt -f cust_feature_cleansing.sql >& cust_feature_cleansing.log
#$ if [ $? -ne 0 ];then    echo exit; else    echo "ok"; fi
echo "---------------------step1/cust_feature_cleansing.sql end ......"
echo "---------------------step1/cust_feature_processing.sql start._2....."
hive -f cust_feature_processing.sql >& cust_feature_processing.log
echo "---------------------step1/cust_feature_processing.sql end ......"

echo "---------------------  cust feature cleansing end   -----------------------"