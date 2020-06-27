#!/bin/bash
yesterday=`date -d -1days '+%Y%m%d'`
pt=$3
if [ `expr length $pt` == 8 ] && date -d $pt +%Y%m%d > /dev/null 2>&1;then
    echo $pt; 
else    
	echo "输入的日期格式不正确，应为yyyymmdd";
	exit 1; 
fi   
beginTime="2020-05-01 00:00:00"
echo "===================== tmp_sis_task_item_cleansing_1begin   ======================="
hive -hivevar pt=$pt -hivevar beginTime=$beginTime -f tmp_sis_task_item_cleansing_1.sql
if [ $? -ne 0 ];then 
	echo "tmp_sis_task_item_cleansing_1.sql 处理失败。code=$?";
    exit 1; 
else
    echo "=====================tmp_sis_task_item_cleansing_2.sql start.2....."
fi
hive -hivevar pt=$pt -hivevar beginTime=$beginTime -f tmp_sis_task_item_cleansing_2.sql
if [ $? -ne 0 ];then 
	echo "tmp_sis_task_item_cleansing_2.sql 处理失败。code=$?";
    exit 1; 
else
    echo "=====================tmp_sis_task_item_processing.sql start.3....."
fi
hive -hivevar pt=$pt -hivevar beginTime=$beginTime -f tmp_sis_task_item_processing.sql