#!/bin/bash
queuename=`awk -F '=' '/\[HIVE\]/{a=1}a==1&&$1~/job.queuename/{print $2;exit}' ../../config.ini`
echo "=====================  DLM feature join begin  2 ======================="
hive -hivevar queuename=$queuename -f tmp_dlm_feature_join_2.sql
if [ $? -ne 0 ];then 
    echo "tmp_dlm_feature_join_2.sql 处理失败。code=$?";
    exit 1; 
else
    echo "=====================tmp_dlm_feature_join_3.sql start_3......"
fi
hive -hivevar queuename=$queuename -f tmp_dlm_feature_join_3.sql
if [ $? -ne 0 ];then 
    echo "tmp_dlm_feature_join_3.sql 处理失败。code=$?";
    exit 1; 
else
    echo "=====================tmp_dlm_feature_join_4.sql start._4....."
fi
hive -hivevar queuename=$queuename -f tmp_dlm_feature_join_4.sql
if [ $? -ne 0 ];then 
    echo "tmp_dlm_feature_join_4.sql 处理失败。code=$?";
    exit 1; 
else
    echo "=====================tmp_leads_pool_status.sql start._5....."
fi
hive -hivevar queuename=$queuename -f app_dlm_wide_info.sql