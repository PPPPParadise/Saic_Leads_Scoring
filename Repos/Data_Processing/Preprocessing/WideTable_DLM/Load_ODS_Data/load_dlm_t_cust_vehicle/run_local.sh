#!/bin/bash
pt=20190901
beginTime="2020-05-01 00:00:00"
echo "---------------------cust_vehicle_feature_cleansing.sql start_1......" 
hive -hivevar pt=$pt -hivevar beginTime=$beginTime -f cust_vehicle_feature_cleansing.sql >& cust_vehicle_feature_cleansing.log
#$ if [ $? -ne 0 ];then    echo exit; else    echo "ok"; fi
echo "---------------------cust_vehicle_feature_cleansing.sql end ......"
