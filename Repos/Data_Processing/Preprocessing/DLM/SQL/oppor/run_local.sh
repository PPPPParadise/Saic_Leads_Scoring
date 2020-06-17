#!/bin/bash
pt=20190901
echo "---------------------oppor_feature_cleansing.sql start_1......" 
hive -hivevar pt=$pt -f oppor_feature_cleansing.sql >& oppor_feature_cleansing.log
#$ if [ $? -ne 0 ];then    echo exit; else    echo "ok"; fi
echo "---------------------oppor_feature_cleansing.sql end ......"
echo "---------------------oppor_feature_processing.sql start._2....."
hive -f oppor_feature_processing.sql >& oppor_feature_processing.log
echo "---------------------oppor_feature_processing.sql end ......"
