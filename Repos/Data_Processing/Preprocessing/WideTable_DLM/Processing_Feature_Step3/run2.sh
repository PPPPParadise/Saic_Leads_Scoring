#!/bin/bash
queuename=`awk -F '=' '/\[HIVE\]/{a=1}a==1&&$1~/job.queuename/{print $2;exit}' ../../config.ini`
echo "=====================  DLM feature procssing 3.1 begin   ======================"
echo "=====================step3 followup_d7.sql start.1....." 
hive -hivevar queuename=$queuename -f followup_d7.sql
echo "=====================step3 followup_d15.sql start.2....."
hive -hivevar queuename=$queuename -f followup_d15.sql
echo "=====================step3 followup_d30.sql start.3....."
hive -hivevar queuename=$queuename -f followup_d30.sql
echo "=====================step3 followup_d60.sql start.4....."
hive -hivevar queuename=$queuename -f followup_d60.sql
echo "=====================step3 followup_d90.sql start.5....."
hive -hivevar queuename=$queuename -f followup_d90.sql
echo "=====================step3 trail_count_d30.sql start.6....."
hive -hivevar queuename=$queuename -f trail_count_d30.sql
echo "=====================step3 trail_count_d90.sql start.7....."
hive -hivevar queuename=$queuename -f trail_count_d90.sql
echo "=====================step3 visit_count_d15.sql start.8....."
hive -hivevar queuename=$queuename -f visit_count_d15.sql
echo "=====================step3 visit_count_d30.sql start.9....."
hive -hivevar queuename=$queuename -f visit_count_d30.sql
echo "=====================step3 visit_count_d90.sql start.10....."
hive -hivevar queuename=$queuename -f visit_count_d90.sql
echo "=====================step3 activity_count_d15.sql start.11....."
hive -hivevar queuename=$queuename -f activity_count_d15.sql
echo "=====================step3 activity_count_d30.sql start.12....."
hive -hivevar queuename=$queuename -f activity_count_d30.sql
echo "=====================step3 activity_count_d60.sql start.13....."
hive -hivevar queuename=$queuename -f activity_count_d60.sql
echo "=====================step3 activity_count_d90.sql start.14....."
hive -hivevar queuename=$queuename -f activity_count_d90.sql