#!bin/sh
#source ~/.bash_profile
SQL_PATH=/home/mam_jupyter/jupyter_dir/artefact/lorraine/sql
LOG_DIR=/home/mam_jupyter/jupyter_dir/artefact/lorraine/sql/logs

echo "---------------------  online data cleaning begin   -----------------------"
echo "---------------------get_flag.sql start......"
hive -f $SQL_PATH/get_flag.sql >& $LOG_DIR/get_flag.log
echo "---------------------get_flag.sql end ......"
echo "---------------------GIO_get_flag_tmp.sql start......"
hive -f $SQL_PATH/GIO_get_flag_tmp.sql >& $LOG_DIR/GIO_get_flag_tmp.log
echo "---------------------GIO_get_flag_tmp.sql end ......"
echo "---------------------GIO_activity_map.sql start......"
hive -f $SQL_PATH/GIO_activity_map.sql >& $LOG_DIR/GIO_activity_map.log
echo "---------------------GIO_activity_map.sql end ......"
echo "---------------------GIO_action_map.sql start......"
hive -f $SQL_PATH/GIO_action_map.sql >& $LOG_DIR/GIO_action_map.log
echo "---------------------GIO_action_map.sql end ......"
echo "---------------------  online ccm feature cleansing finish   -----------------------"






