#!/bin/bash
pt=$3
days=`awk -F '=' '/\[DLM\]/{a=1}a==1&&$1~/days/{print $2;exit}' ../../../config.ini`
queuename=`awk -F '=' '/\[HIVE\]/{a=1}a==1&&$1~/job.queuename/{print $2;exit}' ../../../config.ini`
beginTime=`date -d "$days days ago" +%Y-%m-%d`
if [ `expr length $pt` == 8 ] && date -d $pt +%Y%m%d > /dev/null 2>&1;then
    echo $pt; 
else    
    echo "输入的日期格式不正确，应为yyyymmdd";
    exit 1; 
fi  
echo "======================trial_receive_feature_cleansing.sql start_1......" 
hive -hivevar pt=$pt -hivevar beginTime=$beginTime -hivevar queuename=$queuename -f trial_receive_feature_cleansing.sql
if [ $? -ne 0 ];then 
    echo "trial_receive_feature_cleansing.sql 处理失败。code=$?";
    exit 1; 
else
    echo "======================trial_receive_feature_processing.sql start.2.....";
fi
hive -hivevar queuename=$queuename -f trial_receive_feature_processing.sql