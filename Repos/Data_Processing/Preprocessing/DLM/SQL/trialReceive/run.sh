#!/bin/bash
pt=20190901
echo "---------------------trial_receive_feature_cleansing.sql start_1......" 
hive -hivevar pt=$3 -f trial_receive_feature_cleansing.sql
echo "---------------------trial_receive_feature_processing.sql start._2....."
hive -f trial_receive_feature_processing.sql