#!/bin/bash
queuename=`awk -F '=' '/\[HIVE\]/{a=1}a==1&&$1~/job.queuename/{print $2;exit}' ../../config.ini`
echo "=============================step1 mobile_mapping_processing.sql start_1......" 
hive -hivevar queuename=$queuename -f mobile_mapping_processing.sql
if [ $? -ne 0 ];then 
    echo "处理mobile_mapping_processing.sql 失败。code=$?";
    exit 1; 
else
    echo "=============================step1/deal_flag_feature.sql start.2.....";
fi
#hive -hivevar queuename=$queuename -f deal_flag_feature.sql  以提前run
echo "=============================step1/tmp_dlm_feature_join_1.sql start_3......" 
hive -hivevar queuename=$queuename -f tmp_dlm_feature_join_1.sql
if [ $? -ne 0 ];then 
    echo "处理tmp_dlm_feature_join_1.sql 失败。code=$?";
    exit 1; 
else
    echo echo "=============================step1/tmp_dlm_last_deal_time.sql start_4......"
fi
hive -hivevar queuename=$queuename -f tmp_dlm_last_deal_time.sql