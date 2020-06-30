#!/bin/bash
yesterday=`date -d -1days '+%Y%m%d'`
pt=$3
queuename=`awk -F '=' '/\[HIVE\]/{a=1}a==1&&$1~/job.queuename/{print $2;exit}' ../config.ini`
days=`awk -F '=' '/\[DLM\]/{a=1}a==1&&$1~/days/{print $2;exit}' ../config.ini`  
beginTime=`date -d "$days days ago" +%Y-%m-%d`
if [ `expr length $pt` == 8 ] && date -d $pt +%Y%m%d > /dev/null 2>&1;then
    echo $pt; 
else    
	echo "输入的日期格式不正确，应为yyyymmdd";
	exit 1; 
fi 
echo "===================== tmp_sis_task_item_cleansing_1begin   ======================="
hive -hivevar pt=$pt -hivevar beginTime=$beginTime -hivevar queuename=$queuename -f tmp_sis_task_item_cleansing_1.sql
if [ $? -ne 0 ];then 
	echo "tmp_sis_task_item_cleansing_1.sql 处理失败。code=$?";
    exit 1; 
else
    echo "=====================tmp_sis_task_item_cleansing_2.sql start.2....."
fi
hive -hivevar pt=$pt -hivevar beginTime=$beginTime -hivevar queuename=$queuename -f tmp_sis_task_item_cleansing_2.sql