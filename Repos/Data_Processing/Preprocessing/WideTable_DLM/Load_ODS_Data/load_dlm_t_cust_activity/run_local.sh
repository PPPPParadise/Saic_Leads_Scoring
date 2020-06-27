#!/bin/bash
pt=20190901
beginTime='2019-08-01 00:00:00'
echo "---------------------cust_activity_cleansing.sql start_1......" 
hive -hivevar pt=$pt -hivevar beginTime=$beginTime -f cust_activity_cleansing.sql >& cust_activity_cleansing.log
#$ if [ $? -ne 0 ];then    echo exit; else    echo "ok"; fi
echo "---------------------cust_activity_cleansing.sql end ......"
echo "---------------------cust_activity_processing.sql start._2....."
hive -hivevar queuename=$queuename -f cust_activity_processing.sql >& cust_activity_processing.log
echo "---------------------cust_activity_processing.sql end ......"
