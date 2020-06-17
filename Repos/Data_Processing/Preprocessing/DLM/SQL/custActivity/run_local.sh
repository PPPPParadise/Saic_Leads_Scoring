#!/bin/bash
pt=20190901
echo "---------------------cust_activity_cleansing.sql start_1......" 
hive -hivevar pt=$pt -f cust_activity_cleansing.sql >& cust_activity_cleansing.log
#$ if [ $? -ne 0 ];then    echo exit; else    echo "ok"; fi
echo "---------------------cust_activity_cleansing.sql end ......"
echo "---------------------cust_activity_processing.sql start._2....."
hive -f cust_activity_processing.sql >& cust_activity_processing.log
echo "---------------------cust_activity_processing.sql end ......"
