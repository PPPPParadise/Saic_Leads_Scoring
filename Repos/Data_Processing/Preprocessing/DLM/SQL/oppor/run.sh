#!/bin/bash
pt=20190901
echo "---------------------oppor_feature_cleansing.sql start_1......" 
hive -hivevar pt=$3 -f oppor_feature_cleansing.sql
echo "---------------------oppor_feature_processing.sql start._2....."
hive -f oppor_feature_processing.sql