#!/bin/bash
pt=20190901
echo "---------------------followup_feature_cleansing.sql start_1......" 
hive -hivevar pt=$3 -f followup_feature_cleansing.sql
hive -f followup_feature_processing.sql