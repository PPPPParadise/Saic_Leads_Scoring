#!/bin/bash
pt=$3
beginTime=`awk -F '=' '/\[DLM\]/{a=1}a==1&&$1~/beginTime/{print $2;exit}' ../../../config.ini`
queuename=`awk -F '=' '/\[HIVE\]/{a=1}a==1&&$1~/job.queuename/{print $2;exit}' ../../../config.ini`
if [ `expr length $pt` == 8 ] && date -d $pt +%Y%m%d > /dev/null 2>&1;then
    echo $pt; 
else    
    echo "pt=$pt 输入的日期格式不正确，应为yyyymmdd";
    exit 1; 
fi  
echo "==========================cust_activity_cleansing.sql start_1......" 
hive -hivevar pt=$pt -hivevar beginTime=$beginTime -hivevar queuename=$queuename -f cust_activity_cleansing.sql
if [ $? -ne 0 ];then 
    echo "cust_activity_cleansing.sql 处理失败。code=$?";
    exit 1; 
else
    echo "==========================cust_activity_processing.sql start.2.....";
fi
hive -hivevar queuename=$queuename -f cust_activity_processing.sql