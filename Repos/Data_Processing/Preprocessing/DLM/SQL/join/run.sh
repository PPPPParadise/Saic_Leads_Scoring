#!/bin/bash
echo "---------------------  DLM feature join begin  2 -----------------------"
hive -f dlm_tmp_feature_join_2.sql
echo "---------------------join/dlm_tmp_feature_join_3.sql start_3......"
hive -f dlm_tmp_feature_join_3.sql
echo "---------------------join/dlm_tmp_feature_join_4.sql start._4....."
hive -f dlm_tmp_feature_join_4.sql
echo "---------------------join/mm_dlm_behavior_wide_info.sql start._4....."
hive -f mm_dlm_behavior_wide_info.sql