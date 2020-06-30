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
#if [ $? -ne 0 ];then    echo exit; else    echo "leads_feature_cleansing.sql end ......"; fi
echo "=================gio_finance_count.sql start._2....."
hive -hivevar queuename=$queuename -f gio_finance_count.sql
echo "=================total_browse_time.sql start_1......" 
hive -hivevar pt=$pt -hivevar queuename=$queuename -f total_browse_time.sql
#if [ $? -ne 0 ];then    echo exit; else    echo "step1/cust_feature_cleansing.sql end ......"; fi
echo "=================timeline_behavior.sql start._2...return_code:"$?
hive -hivevar queuename=$queuename -f timeline_behavior.sql