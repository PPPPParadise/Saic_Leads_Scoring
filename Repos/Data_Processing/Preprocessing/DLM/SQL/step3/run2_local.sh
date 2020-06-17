#!bin/sh
LOG_DIR=logs

echo "---------------------  DLM feature procssing 3.1 begin   -----------------------"
echo "---------------------step3/followup_d7.sql start.1....." 
hive -f followup_d7.sql >& $LOG_DIR/followup_d7.log
echo "---------------------step3/followup_d7.sql end ......"

echo "---------------------step3/followup_d15.sql start.2....."
hive -f followup_d15.sql >& $LOG_DIR/followup_d15.log
echo "---------------------step3/followup_d15.sql end ......"

echo "---------------------step3/followup_d30.sql start.3....."
hive -f followup_d30.sql >& $LOG_DIR/followup_d30.log
echo "---------------------step3/followup_d30.sql end ......"

echo "---------------------step3/followup_d60.sql start.4....."
hive -f followup_d60.sql >& $LOG_DIR/followup_d60.log
echo "---------------------step3/followup_d60.sql end ......"

echo "---------------------step3/followup_d90.sql start.5....."
hive -f followup_d90.sql >& $LOG_DIR/followup_d90.log
echo "---------------------step3/followup_d90.sql end ......"

echo "---------------------step3/trail_count_d30.sql start.6....."
hive -f trail_count_d30.sql >& $LOG_DIR/trail_count_d30.log
echo "---------------------step3/trail_count_d30.sql end ......"

echo "---------------------step3/trail_count_d90.sql start.7....."
hive -f trail_count_d90.sql >& $LOG_DIR/trail_count_d90.log
echo "---------------------step3/trail_count_d90.sql end ......"

echo "---------------------step3/visit_count_d15.sql start.8....."
hive -f visit_count_d15.sql >& $LOG_DIR/visit_count_d15.log
echo "---------------------step3/visit_count_d15.sql end ......"

echo "---------------------step3/visit_count_d30.sql start.9....."
hive -f visit_count_d30.sql >& $LOG_DIR/visit_count_d30.log
echo "---------------------step3/visit_count_d30.sql end ......"

echo "---------------------step3/visit_count_d90.sql start.10....."
hive -f visit_count_d90.sql >& $LOG_DIR/visit_count_d90.log
echo "---------------------step3/visit_count_d90.sql end ......"

echo "---------------------step3/activity_count_d15.sql start.11....."
hive -f activity_count_d15.sql >& $LOG_DIR/activity_count_d15.log
echo "---------------------step3/activity_count_d15.sql end ......"

echo "---------------------step3/activity_count_d30.sql start.12....."
hive -f activity_count_d30.sql >& $LOG_DIR/activity_count_d30.log
echo "---------------------step3/activity_count_d30.sql end ......"

echo "---------------------step3/activity_count_d60.sql start.13....."
hive -f activity_count_d60.sql >& $LOG_DIR/activity_count_d60.log
echo "---------------------step3/activity_count_d60.sql end ......"

echo "---------------------step3/activity_count_d90.sql start.14....."
hive -f activity_count_d90.sql >& $LOG_DIR/activity_count_d90.log
echo "---------------------step3/activity_count_d90.sql end ......"

echo "---------------------  DLM feature procssing 3.1 end   -----------------------"