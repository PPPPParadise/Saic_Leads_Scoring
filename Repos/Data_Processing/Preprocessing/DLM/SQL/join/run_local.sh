#!/bin/bash
LOG_DIR=logs

echo "---------------------  DLM feature join begin   -----------------------"

echo "---------------------join/dlm_tmp_feature_join_2.sql start._2....."
hive -f dlm_tmp_feature_join_2.sql >& $LOG_DIR/dlm_tmp_feature_join_2.log
echo "---------------------join/dlm_tmp_feature_join_2.sql end ......"

echo "---------------------join/dlm_tmp_feature_join_3.sql start_3......"
hive -f dlm_tmp_feature_join_3.sql >& $LOG_DIR/dlm_tmp_feature_join_3.log
echo "---------------------join/dlm_tmp_feature_join_3.sql end ......"

echo "---------------------join/dlm_tmp_feature_join_4.sql start._4....."
hive -f dlm_tmp_feature_join_4.sql >& $LOG_DIR/dlm_tmp_feature_join_4.log
echo "---------------------join/dlm_tmp_feature_join_4.sql end ......"

echo "---------------------join/mm_dlm_behavior_wide_info.sql start.._5...."
hive -f mm_dlm_behavior_wide_info.sql >& $LOG_DIR/mm_dlm_behavior_wide_info.log
echo "---------------------join/mm_dlm_behavior_wide_info.sql end ......"
echo "---------------------  DLM feature join end   -----------------------"