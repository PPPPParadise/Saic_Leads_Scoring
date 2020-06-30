#!/bin/bash
queuename=`awk -F '=' '/\[HIVE\]/{a=1}a==1&&$1~/job.queuename/{print $2;exit}' ../../config.ini`
echo "=============================step1 deal_flag_feature.sql start_1......" 
hive -hivevar queuename=$queuename -f deal_flag_feature.sql