#!/bin/bash
pt=20190901
echo "---------------------trial_receive_feature_cleansing.sql start_1......" 
hive -hivevar pt=$pt -f trial_receive_feature_cleansing.sql >& trial_receive_feature_cleansing.log
#$ if [ $? -ne 0 ];then    echo exit; else    echo "ok"; fi
echo "---------------------trial_receive_feature_cleansing.sql end ......"
echo "---------------------trial_receive_feature_processing.sql start._2....."
hive -f trial_receive_feature_processing.sql >& trial_receive_feature_processing.log
echo "---------------------trial_receive_feature_processing.sql end ......"
