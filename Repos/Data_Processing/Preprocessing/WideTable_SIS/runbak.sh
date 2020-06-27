#!/bin/bash
pt=$3
if [ `expr length $pt` == 8 ] && date -d $pt +%Y%m%d > /dev/null 2>&1 ;then
    echo $pt; 
else
	echo "pt=$pt 输入的日期格式不正确，应为yyyymmdd";
	exit 1; 
fi
beginTime="2019-11-01 00:00:00"
echo "===============================SIS task_item cleansing 1.........."
hive -hivevar pt=$pt -hivevar beginTime=$beginTime -f tmp_sis_task_item_cleansing_1.sql 
echo "===============================SIS task_item cleansing 2.........."
hive -hivevar pt=$pt -hivevar beginTime=$beginTime -f tmp_sis_task_item_cleansing_2.sql
echo "===============================SIS task_item processing 3.........."
hive -hivevar pt=$pt -hivevar beginTime=$beginTime -f tmp_sis_task_item_processing.sql