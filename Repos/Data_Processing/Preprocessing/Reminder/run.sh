#!/bin/bash
queuename=`awk -F '=' '/\[HIVE\]/{a=1}a==1&&$1~/job.queuename/{print $2;exit}' ../config.ini`
pt=$3
yesterday=`date -d "$pt 1 days ago" +"%Y%m%d"`
created_time=$(date "+%Y-%m-%d %H:%M:%S")
if [ `expr length $pt` == 8 ] && date -d $pt +%Y%m%d > /dev/null 2>&1; then
    echo $pt; 
else    
    echo "pt=$pt 输入的日期格式不正确，应为yyyymmdd";
    exit 1; 
fi   
echo "===============================tmp_ttl_browse_time.sql"
hive -hivevar pt=$pt -hivevar queuename=$queuename -f tmp_ttl_browse_time.sql
if [ $? -ne 0 ];then 
    echo "tmp_ttl_browse_time.sql 处理失败。code=$?";
    exit 1; 
else
    echo "=====================tmp_ttl_browse_time....."
fi 

echo "===============================tmp_user_intention_changing.sql"
hive -hivevar pt=$pt -hivevar queuename=$queuename -hivevar today=$pt -hivevar yesterday=$yesterday -f tmp_user_intention_changing.sql
if [ $? -ne 0 ];then 
    echo "tmp_user_intention_changing.sql 处理失败。code=$?";
    exit 1; 
else
    echo "=====================tmp_user_intention_changing....."
fi 

echo "===============================tmp_user_leaving_alert.sql"
hive -hivevar queuename=$queuename -hivevar pt=$pt -hivevar yesterday=$yesterday -f tmp_user_leaving_alert.sql
if [ $? -ne 0 ];then 
    echo "tmp_user_leaving_alert.sql 处理失败。code=$?";
    exit 1; 
else
    echo "=====================tmp_user_leaving_alert....."
fi
   
echo "===============================dealer_info.sql"
hive -hivevar pt=$pt -hivevar queuename=$queuename -f dealer_info.sql
if [ $? -ne 0 ];then 
    echo "dealer_info.sql 处理失败。code=$?";
    exit 1; 
else
    echo "=====================dealer_info....."
fi 
echo "===============================tmp_profile_changing_alert.sql"
hive -hivevar queuename=$queuename -hivevar pt=$pt -hivevar yesterday=$yesterday -f tmp_profile_changing_alert.sql
if [ $? -ne 0 ];then 
    echo "tmp_profile_changing_alert.sql 处理失败。code=$?";
    exit 1; 
else
    echo "=====================tmp_profile_changing_alert....."
fi
   

echo "===============================app_mkt_reminder.sql"
hive -hivevar pt=$pt -hivevar queuename=$queuename -f app_mkt_reminder.sql
if [ $? -ne 0 ];then 
    echo "app_mkt_reminder.sql 处理失败。code=$?";
    exit 1; 
else
    echo "=====================app_mkt_reminder....."
fi