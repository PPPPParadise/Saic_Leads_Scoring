#!/bin/bash
queuename=`awk -F '=' '/\[HIVE\]/{a=1}a==1&&$1~/job.queuename/{print $2;exit}' ../config.ini`
pt=$3
if [ `expr length $pt` == 8 ] && date -d $pt +%Y%m%d > /dev/null 2>&1; then
    echo $pt; 
else    
    echo "pt=$pt 输入的日期格式不正确，应为yyyymmdd";
    exit 1; 
fi   
#如果文件夹不存在，创建文件夹
echo "=================begin_time:==================================="
echo "=================dmp_user_info.sql start_1......" 
hive -hivevar queuename=$queuename -f dmp_user_info.sql
if [ $? -ne 0 ];then 
    echo "diff_lasttrail_dealf.sql 处理失败。code=$?";
    exit 1; 
else
    echo "=================gio_finance_count.sql start._2....." 
fi
hive -hivevar queuename=$queuename -f gio_finance_count.sql
if [ $? -ne 0 ];then 
    echo "gio_finance_count.sql 处理失败。code=$?";
    exit 1; 
else
    echo "=================total_browse_time.sql start_1......"
fi
hive -hivevar pt=$pt -hivevar queuename=$queuename -f total_browse_time.sql
if [ $? -ne 0 ];then 
    echo "total_browse_time.sql 处理失败。code=$?";
    exit 1; 
else
    echo "================timeline_behavior.sql start.8....." 
fi
hive -hivevar queuename=$queuename -f timeline_behavior.sql