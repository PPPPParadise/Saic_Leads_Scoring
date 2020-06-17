#!/bin/bash
pt=20190901
echo "---------------------cust_vehicle_feature_cleansing.sql start_1......" 
hive -hivevar pt=$pt -f cust_vehicle_feature_cleansing.sql >& cust_vehicle_feature_cleansing.log
#$ if [ $? -ne 0 ];then    echo exit; else    echo "ok"; fi
echo "---------------------cust_vehicle_feature_cleansing.sql end ......"
