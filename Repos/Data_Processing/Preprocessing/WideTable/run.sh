#!/bin/bash
queuename=`awk -F '=' '/\[HIVE\]/{a=1}a==1&&$1~/job.queuename/{print $2;exit}' ../config.ini`
pt=$3
if [ `expr length $pt` == 8 ] && date -d $pt +%Y%m%d > /dev/null 2>&1; then
    echo $pt; 
else
	echo "pt=$pt 输入的日期格式不正确，应为yyyymmdd";
	exit 1; 
fi
echo "---------------------mm_big_wide_info.sql start_1....................."
hive -hivevar pt=$pt -hivevar queuename=$queuename -f sql/app_big_wide_info.sql
