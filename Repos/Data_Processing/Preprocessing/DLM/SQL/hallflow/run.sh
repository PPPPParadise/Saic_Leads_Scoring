#!/bin/bash
pt=20190901
echo "---------------------hall_flow_feature_cleansing.sql start_1......" 
hive -hivevar pt=$3 -f hall_flow_feature_cleansing.sql
hive -f hall_flow_feature_processing.sql
