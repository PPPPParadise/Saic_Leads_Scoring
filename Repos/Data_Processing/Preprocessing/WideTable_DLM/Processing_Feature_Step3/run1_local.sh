#!/bin/bash
Log_DIR=logs
queuename=`awk -F '=' '/\[HIVE\]/{a=1}a==1&&$1~/job.queuename/{print $2;exit}' ../../config.ini`
echo "======================  DLM feature procssing 3 begin   ======================"
echo "======================step3/diff_dealf_firvisit.sql start.1....." 
hive -hivevar queuename=$queuename -f diff_dealf_firvisit.sql >& $LOG_DIR/diff_dealf_firvisit.log
echo "======================step3/diff_dealf_firvisit.sql end ......"

echo "======================step3/diff_fircard_dealf.sql start.2....."
hive -hivevar queuename=$queuename -f diff_fircard_dealf.sql >& $LOG_DIR/diff_fircard_dealf.log
echo "======================step3/diff_fircard_dealf.sql end ......"

echo "======================step3/diff_firfollow_dealf.sql start.3....."
hive -hivevar queuename=$queuename -f diff_firfollow_dealf.sql >& $LOG_DIR/diff_firfollow_dealf.log
echo "======================step3/diff_firfollow_dealf.sql end ......"

echo "======================step3/diff_firstlead_dealf.sql start.4....."
hive -hivevar queuename=$queuename -f diff_firstlead_dealf.sql >& $LOG_DIR/diff_firstlead_dealf.log
echo "======================step3/diff_firstlead_dealf.sql end ......"

echo "======================step3/diff_last_dfail_dealf.sql start.5....."
hive -hivevar queuename=$queuename -f diff_last_dfail_dealf.sql >& $LOG_DIR/diff_last_dfail_dealf.log
echo "======================step3/diff_last_dfail_dealf.sql end ......"

echo "======================step3/diff_lastlead_dealf.sql start.6....."
hive -hivevar queuename=$queuename -f diff_lastlead_dealf.sql >& $LOG_DIR/diff_lastlead_dealf.log
echo "======================step3/diff_lastlead_dealf.sql end ......"

echo "======================step3/diff_lasttrail_dealf.sql start.7....."
hive -hivevar queuename=$queuename -f diff_lasttrail_dealf.sql >& $LOG_DIR/diff_lasttrail_dealf.log
echo "======================step3/diff_lasttrail_dealf.sql end ......"

echo "======================step3/join_some_diff_feature.sql start.8....."
hive -hivevar queuename=$queuename -f join_some_diff_feature.sql >& $LOG_DIR/join_some_diff_feature.log
echo "======================step3/join_some_diff_feature.sql end ......"

echo "======================step3/fir_order_leads_diff.sql start.9....."
hive -hivevar queuename=$queuename -f fir_order_leads_diff.sql >& $LOG_DIR/fir_order_leads_diff.log
echo "======================step3/fir_order_leads_diff.sql end ......"

echo "======================step3/fir_order_visit_diff.sql start.10....."
hive -hivevar queuename=$queuename -f fir_order_visit_diff.sql >& $LOG_DIR/fir_order_visit_diff.log
echo "======================step3/fir_order_visit_diff.sql end ......"

echo "======================step3/fir_order_trail_diff.sql start.11....."
hive -hivevar queuename=$queuename -f fir_order_trail_diff.sql >& $LOG_DIR/fir_order_trail_diff.log
echo "======================step3/fir_order_trail_diff.sql end ......"

echo "======================step3/fir_activity_dealf_diff.sql start.12....."
hive -hivevar queuename=$queuename -f fir_activity_dealf_diff.sql >& $LOG_DIR/fir_activity_dealf_diff.log
echo "======================step3/fir_activity_dealf_diff.sql end ......"

echo "======================step3/last_activity_dealf_diff.sql start.13....."
hive -hivevar queuename=$queuename -f last_activity_dealf_diff.sql >& $LOG_DIR/last_activity_dealf_diff.log
echo "======================step3/last_activity_dealf_diff.sql end ......"

echo "======================step3/fir_lead_dealf_diff_y.sql start.14....."
hive -hivevar queuename=$queuename -f fir_lead_dealf_diff_y.sql >& $LOG_DIR/fir_lead_dealf_diff_y.log
echo "======================step3/fir_lead_dealf_diff_y.sql end ......"

echo "======================step3/natural_visit.sql start.15....."
hive -hivevar queuename=$queuename -f natural_visit.sql >& $LOG_DIR/natural_visit.log
echo "======================step3/natural_visit.sql end ......"

echo "======================  DLM feature procssing 3 end   ======================"
