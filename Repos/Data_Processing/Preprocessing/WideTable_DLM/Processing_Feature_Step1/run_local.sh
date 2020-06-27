#!/bin/bash
LOG_DIR=logs
queuename=`awk -F '=' '/\[HIVE\]/{a=1}a==1&&$1~/job.queuename/{print $2;exit}' ../../config.ini`
echo "---------------------step1 mobile_mapping_processing.sql start_1......" 
hive -hivevar queuename=$queuename -f mobile_mapping_processing.sql >& $LOG_DIR/mobile_mapping_processing.log
#$ if [ $? -ne 0 ];then    echo exit; else    echo "ok"; fi
echo "---------------------step1 mobile_mapping_processing.sql end ......"

echo "---------------------step1/deal_flag_feature.sql start.2....."
hive -hivevar queuename=$queuename -f deal_flag_feature.sql >& $LOG_DIR/deal_flag_feature.log
echo "---------------------step1/deal_flag_feature.sql end ......"

echo "---------------------step1/tmp_dlm_feature_join_1.sql start_3......" 
hive -hivevar queuename=$queuename -f tmp_dlm_feature_join_1.sql >& $LOG_DIR/tmp_dlm_feature_join_1.log 
echo "---------------------step1/tmp_dlm_feature_join_1.sql end ......"

echo "---------------------step1/tmp_dlm_last_deal_time.sql start_4......" 
hive -hivevar queuename=$queuename -f tmp_dlm_last_deal_time.sql >& $LOG_DIR/tmp_dlm_last_deal_time.log 
echo "---------------------step1/tmp_dlm_last_deal_time.sql end ......"
