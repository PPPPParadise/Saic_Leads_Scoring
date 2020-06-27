#!/bin/bash
Log_DIR=logs
queuename=`awk -F '=' '/\[HIVE\]/{a=1}a==1&&$1~/job.queuename/{print $2;exit}' ../../config.ini`
echo "---------------------step2/avg_firleads_firvisit_diff.sql start.1....." 
hive -hivevar queuename=$queuename -f avg_firleads_firvisit_diff.sql >& $LOG_DIR/avg_firleads_firvisit_diff.log
echo "---------------------step2/avg_firleads_firvisit_diff.sql end ......"

echo "---------------------step2/avg_fircard_firvisit_diff.sql start.2....."
hive -hivevar queuename=$queuename -f avg_fircard_firvisit_diff.sql >& $LOG_DIR/avg_fircard_firvisit_diff.log
echo "---------------------step2/avg_fircard_firvisit_diff.sql end ......"

echo "---------------------step2/avg_fir_sec_visit_diff.sql start.3....."
hive -hivevar queuename=$queuename -f avg_fir_sec_visit_diff.sql >& $LOG_DIR/avg_fir_sec_visit_diff.log
echo "---------------------step2/avg_fir_sec_visit_diff.sql end ......"

echo "---------------------step2/dealf_succ_firvisit_diff.sql start.4....."
hive -hivevar queuename=$queuename -f dealf_succ_firvisit_diff.sql >& $LOG_DIR/dealf_succ_firvisit_diff.log
echo "---------------------step2/dealf_succ_firvisit_diff.sql end ......"

echo "---------------------step2/dealf_succ_lastvisit_diff.sql start.5....."
hive -hivevar queuename=$queuename -f dealf_succ_lastvisit_diff.sql >& $LOG_DIR/dealf_succ_lastvisit_diff.log
echo "---------------------step2/dealf_succ_lastvisit_diff.sql end ......"

echo "---------------------step2/has_deliver_history.sql start.6....."
hive -hivevar queuename=$queuename -f has_deliver_history.sql >& $LOG_DIR/has_deliver_history.log
echo "---------------------step2/has_deliver_history.sql end ......"


