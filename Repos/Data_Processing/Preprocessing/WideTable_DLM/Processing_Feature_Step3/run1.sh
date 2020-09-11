#!/bin/bash
queuename=`awk -F '=' '/\[HIVE\]/{a=1}a==1&&$1~/job.queuename/{print $2;exit}' ../../config.ini`
echo "================  DLM feature procssing 3 begin   =================="
echo "================step3 diff_dealf_firvisit.sql start.1....."
hive -hivevar queuename=$queuename -f diff_dealf_firvisit.sql
if [ $? -ne 0 ];then 
    echo "diff_dealf_firvisit.sql 处理失败。code=$?";
    exit 1; 
else
    echo "================step3 diff_fircard_dealf.sql start.2....."
fi
hive -hivevar queuename=$queuename -f diff_fircard_dealf.sql
if [ $? -ne 0 ];then 
    echo "diff_fircard_dealf.sql 处理失败。code=$?";
    exit 1; 
else
    echo "================step3 diff_firfollow_dealf.sql start.3....."
fi
hive -hivevar queuename=$queuename -f diff_firfollow_dealf.sql
if [ $? -ne 0 ];then 
    echo "diff_firfollow_dealf.sql 处理失败。code=$?";
    exit 1; 
else
    echo "================step3 diff_firstlead_dealf.sql start.4....." 
fi
hive -hivevar queuename=$queuename -f diff_firstlead_dealf.sql
if [ $? -ne 0 ];then 
    echo "diff_firstlead_dealf.sql 处理失败。code=$?";
    exit 1; 
else
    echo "================step3 diff_last_dfail_dealf.sql start.5....." 
fi
hive -hivevar queuename=$queuename -f diff_last_dfail_dealf.sql
if [ $? -ne 0 ];then 
    echo "diff_last_dfail_dealf.sql 处理失败。code=$?";
    exit 1; 
else
    echo "================step3 diff_lastlead_dealf.sql start.6....." 
fi
hive -hivevar queuename=$queuename -f diff_lastlead_dealf.sql
if [ $? -ne 0 ];then 
    echo "diff_lastlead_dealf.sql 处理失败。code=$?";
    exit 1; 
else
    echo "================step3 diff_lasttrail_dealf.sql start.7....." 
fi
hive -hivevar queuename=$queuename -f diff_lasttrail_dealf.sql
if [ $? -ne 0 ];then 
    echo "diff_lasttrail_dealf.sql 处理失败。code=$?";
    exit 1; 
else
    echo "================step3 join_some_diff_feature.sql start.8....." 
fi
hive -hivevar queuename=$queuename -f join_some_diff_feature.sql
if [ $? -ne 0 ];then 
    echo "join_some_diff_feature.sql 处理失败。code=$?";
    exit 1; 
else
    echo "================step3 fir_order_leads_diff.sql start.9....." 
fi
hive -hivevar queuename=$queuename -f fir_order_leads_diff.sql
if [ $? -ne 0 ];then 
    echo "fir_order_leads_diff.sql 处理失败。code=$?";
    exit 1; 
else
    echo "================step3 fir_order_visit_diff.sql start.10....." 
fi
hive -hivevar queuename=$queuename -f fir_order_visit_diff.sql
if [ $? -ne 0 ];then 
    echo "fir_order_visit_diff.sql 处理失败。code=$?";
    exit 1; 
else
    echo "================step3 fir_order_trail_diff.sql start.11....." 
fi
hive -hivevar queuename=$queuename -f fir_order_trail_diff.sql
if [ $? -ne 0 ];then 
    echo "fir_order_trail_diff.sql 处理失败。code=$?";
    exit 1; 
else
    echo "================step3 fir_activity_dealf_diff.sql start.12....." 
fi
hive -hivevar queuename=$queuename -f fir_activity_dealf_diff.sql
if [ $? -ne 0 ];then 
    echo "fir_activity_dealf_diff.sql 处理失败。code=$?";
    exit 1; 
else
    echo "================step3 last_activity_dealf_diff.sql start.13....." 
fi
hive -hivevar queuename=$queuename -f last_activity_dealf_diff.sql
if [ $? -ne 0 ];then 
    echo "last_activity_dealf_diff.sql 处理失败。code=$?";
    exit 1; 
else
    echo "================step3 fir_lead_dealf_diff_y.sql start.14....."
fi
hive -hivevar queuename=$queuename -f fir_lead_dealf_diff_y.sql
if [ $? -ne 0 ];then 
    echo "fir_lead_dealf_diff_y.sql 处理失败。code=$?";
    exit 1; 
else
    echo "================step3 natural_visit.sql start.15....."
fi 
hive -hivevar queuename=$queuename -f natural_visit.sql