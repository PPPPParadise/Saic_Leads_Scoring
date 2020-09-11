#!/bin/bash
queuename=`awk -F '=' '/\[HIVE\]/{a=1}a==1&&$1~/job.queuename/{print $2;exit}' ../../config.ini`
echo "=====================  DLM feature procssing 3.1 begin   ======================"
echo "=====================step3 followup_d7.sql start.1....." 
hive -hivevar queuename=$queuename -f followup_d7.sql
if [ $? -ne 0 ];then 
    echo "followup_d7.sql 处理失败。code=$?";
    exit 1; 
else
    echo "=====================step3 followup_d15.sql start.2....."
fi
hive -hivevar queuename=$queuename -f followup_d15.sql
if [ $? -ne 0 ];then 
    echo "followup_d15.sql 处理失败。code=$?";
    exit 1; 
else
    echo "=====================step3 followup_d30.sql start.3....."
fi
hive -hivevar queuename=$queuename -f followup_d30.sql
if [ $? -ne 0 ];then 
    echo "followup_d30.sql 处理失败。code=$?";
    exit 1; 
else
    echo "=====================step3 followup_d60.sql start.4....."
fi
hive -hivevar queuename=$queuename -f followup_d60.sql
if [ $? -ne 0 ];then 
    echo "followup_d60.sql 处理失败。code=$?";
    exit 1; 
else
    echo "=====================step3 followup_d90.sql start.5....."
fi
hive -hivevar queuename=$queuename -f followup_d90.sql
if [ $? -ne 0 ];then 
    echo "followup_d90.sql 处理失败。code=$?";
    exit 1; 
else
    echo "=====================step3 trail_count_d30.sql start.6....."
fi
hive -hivevar queuename=$queuename -f trail_count_d30.sql
if [ $? -ne 0 ];then 
    echo "trail_count_d30.sql 处理失败。code=$?";
    exit 1; 
else
    echo "=====================step3 trail_count_d90.sql start.7....."
fi
hive -hivevar queuename=$queuename -f trail_count_d90.sql
if [ $? -ne 0 ];then 
    echo "trail_count_d90.sql 处理失败。code=$?";
    exit 1; 
else
    echo "=====================step3 visit_count_d15.sql start.8....."
fi
hive -hivevar queuename=$queuename -f visit_count_d15.sql
if [ $? -ne 0 ];then 
    echo "visit_count_d15.sql 处理失败。code=$?";
    exit 1; 
else
    echo "=====================step3 visit_count_d30.sql start.9....."
fi
hive -hivevar queuename=$queuename -f visit_count_d30.sql
if [ $? -ne 0 ];then 
    echo "visit_count_d30.sql 处理失败。code=$?";
    exit 1; 
else
    echo "=====================step3 visit_count_d90.sql start.10....."
fi
hive -hivevar queuename=$queuename -f visit_count_d90.sql
if [ $? -ne 0 ];then 
    echo "visit_count_d90.sql 处理失败。code=$?";
    exit 1; 
else
    echo "=====================step3 activity_count_d15.sql start.11....."
fi
hive -hivevar queuename=$queuename -f activity_count_d15.sql
if [ $? -ne 0 ];then 
    echo "activity_count_d15.sql 处理失败。code=$?";
    exit 1; 
else
    echo "=====================step3 activity_count_d30.sql start.12....."
fi
hive -hivevar queuename=$queuename -f activity_count_d30.sql
if [ $? -ne 0 ];then 
    echo "activity_count_d30.sql 处理失败。code=$?";
    exit 1; 
else
    echo "=====================step3 activity_count_d60.sql start.13....."
fi
hive -hivevar queuename=$queuename -f activity_count_d60.sql
if [ $? -ne 0 ];then 
    echo "activity_count_d60.sql 处理失败。code=$?";
    exit 1; 
else
    echo "=====================step3 activity_count_d90.sql start.14....."
fi
hive -hivevar queuename=$queuename -f activity_count_d90.sql