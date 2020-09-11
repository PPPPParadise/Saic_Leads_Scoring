#!/bin/bash
queuename=`awk -F '=' '/\[HIVE\]/{a=1}a==1&&$1~/job.queuename/{print $2;exit}' ../../config.ini`
pt=$3
if [ `expr length $pt` == 8 ] && date -d $pt +%Y%m%d > /dev/null 2>&1; then
    echo $pt; 
else    
    echo "pt=$pt 输入的日期格式不正确，应为yyyymmdd";
    exit 1; 
fi   
echo "====================-cdp_middle_tb.sql start_1......" 
hive -hivevar pt=$pt -hivevar queuename=$queuename -f cdp_middle_tb.sql
if [ $? -ne 0 ];then 
    echo "cdp_middle_tb.sql 处理失败。code=$?";
    exit 1; 
else
    echo "====================-cdp_mapping_tb.sql start._2....."
fi
hive -hivevar pt=$pt -hivevar queuename=$queuename -f cdp_mapping_tb.sql
if [ $? -ne 0 ];then 
    echo "cdp_mapping_tb.sql 处理失败。code=$?";
    exit 1; 
else
    echo "====================-cdp_final_tb.sql start._3....."
fi
hive -hivevar queuename=$queuename -f cdp_final_tb.sql