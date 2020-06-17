#!/bin/bash
Log_DIR=logs

echo "---------------------step2/avg_firleads_firvisit_diff.sql start.1....." 
hive -f avg_firleads_firvisit_diff.sql >& $LOG_DIR/avg_firleads_firvisit_diff.log
echo "---------------------step2/avg_firleads_firvisit_diff.sql end ......"

echo "---------------------step2/avg_fircard_firvisit_diff.sql start.2....."
hive -f avg_fircard_firvisit_diff.sql >& $LOG_DIR/avg_fircard_firvisit_diff.log
echo "---------------------step2/avg_fircard_firvisit_diff.sql end ......"

echo "---------------------step2/avg_fir_sec_visit_diff.sql start.3....."
hive -f avg_fir_sec_visit_diff.sql >& $LOG_DIR/avg_fir_sec_visit_diff.log
echo "---------------------step2/avg_fir_sec_visit_diff.sql end ......"

echo "---------------------step2/dealf_succ_firvisit_diff.sql start.4....."
hive -f dealf_succ_firvisit_diff.sql >& $LOG_DIR/dealf_succ_firvisit_diff.log
echo "---------------------step2/dealf_succ_firvisit_diff.sql end ......"

echo "---------------------step2/dealf_succ_lastvisit_diff.sql start.5....."
hive -f dealf_succ_lastvisit_diff.sql >& $LOG_DIR/dealf_succ_lastvisit_diff.log
echo "---------------------step2/dealf_succ_lastvisit_diff.sql end ......"

echo "---------------------step2/has_deliver_history.sql start.6....."
hive -f has_deliver_history.sql >& $LOG_DIR/has_deliver_history.log
echo "---------------------step2/has_deliver_history.sql end ......"


