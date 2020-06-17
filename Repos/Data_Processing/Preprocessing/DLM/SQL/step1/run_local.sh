#!/bin/bash
LOG_DIR=logs

echo "---------------------step1 mobile_mapping_processing.sql start_1......" 
hive -f mobile_mapping_processing.sql >& $LOG_DIR/mobile_mapping_processing.log
#$ if [ $? -ne 0 ];then    echo exit; else    echo "ok"; fi
echo "---------------------step1 mobile_mapping_processing.sql end ......"

echo "---------------------step1/deal_flag_feature.sql start.2....."
hive -f deal_flag_feature.sql >& $LOG_DIR/deal_flag_feature.log
echo "---------------------step1/deal_flag_feature.sql end ......"

echo "---------------------step1/dlm_tmp_feature_join_1.sql start_3......" 
hive -f dlm_tmp_feature_join_1.sql >& $LOG_DIR/dlm_tmp_feature_join_1.log 
echo "---------------------step1/dlm_tmp_feature_join_1.sql end ......"

echo "---------------------step1/dlm_tmp_last_deal_time.sql start_4......" 
hive -f dlm_tmp_last_deal_time.sql >& $LOG_DIR/dlm_tmp_last_deal_time.log 
echo "---------------------step1/dlm_tmp_last_deal_time.sql end ......"
