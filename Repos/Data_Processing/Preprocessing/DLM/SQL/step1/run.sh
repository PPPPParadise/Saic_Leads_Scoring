#!/bin/bash
echo "---------------------step1 mobile_mapping_processing.sql start_1......" 
hive -f mobile_mapping_processing.sql
echo "---------------------step1/deal_flag_feature.sql start.2....."
hive -f deal_flag_feature.sql
echo "---------------------step1/dlm_tmp_feature_join_1.sql start_3......" 
hive -f dlm_tmp_feature_join_1.sql
echo "---------------------step1/dlm_tmp_last_deal_time.sql start_4......" 
hive -f dlm_tmp_last_deal_time.sql