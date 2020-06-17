#!/bin/bash
pt=20190901
echo "---------------------  cust feature cleansing begin   -----------------------"
hive -hivevar pt=$3 -f cust_feature_cleansing.sql
hive -f cust_feature_processing.sql