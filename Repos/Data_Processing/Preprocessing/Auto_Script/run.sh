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
echo "===============================tmp_auto_script_step1.sql"
hive -hivevar pt=$pt -hivevar queuename=$queuename -f tmp_auto_script_step1.sql
if [ $? -ne 0 ];then 
    echo "tmp_auto_script_step1.sql 处理失败。code=$?";
    exit 1; 
else
    echo "=====================tmp_auto_script_step1....."
fi 
echo "===============================tmp_auto_script_step2.sql"
hive -hivevar pt=$pt -hivevar queuename=$queuename -f tmp_auto_script_step2.sql
if [ $? -ne 0 ];then 
    echo "tmp_auto_script_step2.sql 处理失败。code=$?";
    exit 1; 
else
    echo "=====================tmp_auto_script_step2....."
fi   

echo "===============================followup_auto_script.sql"
hive -hivevar pt=$pt -hivevar queuename=$queuename -f followup_auto_script.sql
if [ $? -ne 0 ];then
    echo "followup_auto_script.sql 处理失败。code=$?";
    exit 1;
else
    echo "=====================followup_auto_script.sql....."
fi

echo "===============================fail_reason_script.sql"
hive -hivevar pt=$pt -hivevar queuename=$queuename -f fail_reason_script.sql
if [ $? -ne 0 ];then 
    echo "fail_reason_script.sql 处理失败。code=$?";
    exit 1; 
else
    echo "=====================fail_reason_script....."
fi

echo "===============================app_auto_script.sql"
hive -hivevar pt=$pt -hivevar queuename=$queuename -f app_auto_script.sql
if [ $? -ne 0 ];then 
    echo "app_auto_script.sql 处理失败。code=$?";
    exit 1; 
else
    echo "=====================app_auto_script....."
fi
