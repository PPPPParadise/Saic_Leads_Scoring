#!bin/sh
#source ~/.bash_profile
SQL_PATH=/home/mam_jupyter/jupyter_dir/artefact/lorraine/sql
LOG_DIR=/home/mam_jupyter/jupyter_dir/artefact/lorraine/sql/logs

echo "---------------------  gio feature mapping with id and flag begin   -----------------------"
echo "---------------------step1/GIO_deal_flag_map.sql start......"
hive -f $SQL_PATH/GIO_deal_flag_map.sql >& $LOG_DIR/GIO_deal_flag_map.log
echo "---------------------step1/GIO_deal_flag_map.sql end ......"
echo "---------------------step1/GIO_action_map.sql start......"
hive -f $SQL_PATH/GIO_action_map.sql >& $LOG_DIR/GIO_action_map.log
echo "---------------------step1/GIO_action_map.sql end ......"
echo "---------------------step1/GIO_activity_map.sql start......"
hive -f $SQL_PATH/GIO_activity_map.sql >& $LOG_DIR/GIO_activity_map.log
echo "---------------------step1/GIO_activity_map.sql end ......"
echo "--------------------- gio feature mapping with id and flagl end ......"




