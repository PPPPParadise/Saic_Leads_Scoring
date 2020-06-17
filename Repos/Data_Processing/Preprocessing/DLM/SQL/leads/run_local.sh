#!/bin/bash
pt=20190901
echo "---------------------leads_feature_cleansing.sql start_1......" 
hive -hivevar pt=$pt -f leads_feature_cleansing.sql >& leads_feature_cleansing.log
#$ if [ $? -ne 0 ];then    echo exit; else    echo "ok"; fi
echo "---------------------leads_feature_cleansing.sql end ......"
echo "---------------------leads_feature_processing.sql start._2....."
hive -f leads_feature_processing.sql >& leads_feature_processing.log
echo "---------------------leads_feature_processing.sql end ......"
