#!/bin/bash
pt=20190901
echo "---------------------deliver_feature_cleansing.sql start_1......" 
hive -hivevar pt=$3 -f deliver_feature_cleansing.sql
hive -f deliver_feature_processing.sql