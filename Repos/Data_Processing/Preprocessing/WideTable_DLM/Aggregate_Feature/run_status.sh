#!/bin/bash
queuename=`awk -F '=' '/\[HIVE\]/{a=1}a==1&&$1~/job.queuename/{print $2;exit}' ../../config.ini`
echo "=====================  DLM feature join begin  2 ======================="
hive -hivevar queuename=$queuename -f tmp_leads_pool_status.sql