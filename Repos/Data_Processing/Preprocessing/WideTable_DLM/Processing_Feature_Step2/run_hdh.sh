#!/bin/bash
queuename=`awk -F '=' '/\[HIVE\]/{a=1}a==1&&$1~/job.queuename/{print $2;exit}' ../../config.ini`
echo "---------------------has_deliver_history.sql start.1....."
hive -hivevar queuename=$queuename -f has_deliver_history.sql