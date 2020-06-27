#!/bin/bash
queuename=`awk -F '=' '/\[HIVE\]/{a=1}a==1&&$1~/job.queuename/{print $2;exit}' ../../config.ini`
echo "================avg_firleads_firvisit_diff.sql start.1....."
hive -hivevar queuename=$queuename -f avg_firleads_firvisit_diff.sql
if [ $? -ne 0 ];then 
    echo "avg_firleads_firvisit_diff.sql 处理失败。code=$?";
    exit 1; 
else
    echo "================leads_feature_processing.sql start.2....."
fi
hive -hivevar queuename=$queuename -f avg_fircard_firvisit_diff.sql
if [ $? -ne 0 ];then 
    echo "avg_fircard_firvisit_diff.sql 处理失败。code=$?";
    exit 1; 
else
    echo "================avg_fir_sec_visit_diff.sql start.3....."
fi
hive -hivevar queuename=$queuename -f avg_fir_sec_visit_diff.sql
if [ $? -ne 0 ];then 
    echo "avg_fir_sec_visit_diff.sql 处理失败。code=$?";
    exit 1; 
else
    echo "================dealf_succ_firvisit_diff.sql start.4....."
fi
hive -hivevar queuename=$queuename -f dealf_succ_firvisit_diff.sql
if [ $? -ne 0 ];then 
    echo "dealf_succ_firvisit_diff.sql 处理失败。code=$?";
    exit 1; 
else
    echo "================dealf_succ_lastvisit_diff.sql start.5....."
fi
hive -hivevar queuename=$queuename -f dealf_succ_lastvisit_diff.sql