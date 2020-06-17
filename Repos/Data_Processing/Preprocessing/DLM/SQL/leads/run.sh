#!/bin/bash
pt=20190901
echo "---------------------leads_feature_cleansing.sql start_1......" 
hive -hivevar pt=$3 -f leads_feature_cleansing.sql
echo "---------------------leads_feature_processing.sql start._2....."
hive -f leads_feature_processing.sql