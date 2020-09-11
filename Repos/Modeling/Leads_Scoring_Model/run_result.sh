#!/bin/bash
queuename=malg
pt=$3
if [ `expr length $pt` == 8 ] && date -d $pt +%Y%m%d > /dev/null 2>&1; then
    echo $pt; 
else
    echo "pt=$pt 输入的日期格式不正确，应为yyyymmdd";
    exit 1; 
fi
hive -hivevar pt=$pt -hivevar queuename=$queuename -f SQL/insert_app_model_result.sql