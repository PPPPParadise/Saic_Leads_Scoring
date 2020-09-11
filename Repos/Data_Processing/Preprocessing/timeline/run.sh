#!/bin/bash
queuename=`awk -F '=' '/\[HIVE\]/{a=1}a==1&&$1~/job.queuename/{print $2;exit}' ../config.ini`
pt=$3
created_time=$(date "+%Y-%m-%d %H:%M:%S")
if [ `expr length $pt` == 8 ] && date -d $pt +%Y%m%d > /dev/null 2>&1; then
    echo $pt; 
else    
    echo "pt=$pt 输入的日期格式不正确，应为yyyymmdd";
    exit 1; 
fi   
echo "===============================online_timeline.sql"
hive -hivevar pt=$pt -hivevar queuename=$queuename -f online_timeline.sql
if [ $? -ne 0 ];then 
    echo "online_timeline.sql 处理失败。code=$?";
    exit 1; 
else
    echo "=====================online_timeline....."
fi 
