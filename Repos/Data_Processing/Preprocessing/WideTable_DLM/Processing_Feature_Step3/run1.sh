#!/bin/bash
queuename=`awk -F '=' '/\[HIVE\]/{a=1}a==1&&$1~/job.queuename/{print $2;exit}' ../../config.ini`
echo "================  DLM feature procssing 3 begin   =================="
echo "================step3 diff_dealf_firvisit.sql start.1....."
hive -hivevar queuename=$queuename -f diff_dealf_firvisit.sql
echo "================step3 diff_fircard_dealf.sql start.2....."
hive -hivevar queuename=$queuename -f diff_fircard_dealf.sql
echo "================step3 diff_firfollow_dealf.sql start.3....."
hive -hivevar queuename=$queuename -f diff_firfollow_dealf.sql
echo "================step3 diff_firstlead_dealf.sql start.4....."
hive -hivevar queuename=$queuename -f diff_firstlead_dealf.sql
echo "================step3 diff_last_dfail_dealf.sql start.5....."
hive -hivevar queuename=$queuename -f diff_last_dfail_dealf.sql
echo "================step3 diff_lastlead_dealf.sql start.6....."
hive -hivevar queuename=$queuename -f diff_lastlead_dealf.sql
echo "================step3 diff_lasttrail_dealf.sql start.7....."
hive -hivevar queuename=$queuename -f diff_lasttrail_dealf.sql
echo "================step3 join_some_diff_feature.sql start.8....."
hive -hivevar queuename=$queuename -f join_some_diff_feature.sql
echo "================step3 fir_order_leads_diff.sql start.9....."
hive -hivevar queuename=$queuename -f fir_order_leads_diff.sql
echo "================step3 fir_order_visit_diff.sql start.10....."
hive -hivevar queuename=$queuename -f fir_order_visit_diff.sql
echo "================step3 fir_order_trail_diff.sql start.11....."
hive -hivevar queuename=$queuename -f fir_order_trail_diff.sql
echo "================step3 fir_activity_dealf_diff.sql start.12....."
hive -hivevar queuename=$queuename -f fir_activity_dealf_diff.sql
echo "================step3 last_activity_dealf_diff.sql start.13....."
hive -hivevar queuename=$queuename -f last_activity_dealf_diff.sql
echo "================step3 fir_lead_dealf_diff_y.sql start.14....."
hive -hivevar queuename=$queuename -f fir_lead_dealf_diff_y.sql
echo "================step3 natural_visit.sql start.15....."
hive -hivevar queuename=$queuename -f natural_visit.sql