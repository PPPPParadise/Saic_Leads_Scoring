#!/bin/bash
pt=20190901
echo "---------------------cust_activity_cleansing.sql start_1......" 
hive -hivevar pt=$3 -f cust_activity_cleansing.sql
hive -f cust_activity_processing.sql