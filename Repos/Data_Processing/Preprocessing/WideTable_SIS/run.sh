#!/bin/bash
yesterday=`date -d -1days '+%Y%m%d'`
queuename=`awk -F '=' '/\[HIVE\]/{a=1}a==1&&$1~/job.queuename/{print $2;exit}' ../config.ini`
days=`awk -F '=' '/\[DLM\]/{a=1}a==1&&$1~/days/{print $2;exit}' ../config.ini`  
beginTime=`date -d "$days days ago" +%Y-%m-%d`
echo "===================== tmp_sis_task_item_processing begin ======================="
hive -hivevar queuename=$queuename -f tmp_sis_task_item_processing.sql