#!/bin/bash
pt=20190901
echo "---------------------fail_feature_cleansing.sql start_1......" 
hive -hivevar pt=$3 -f fail_feature_cleansing.sql
hive -f fail_feature_processing.sql