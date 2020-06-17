#!bin/sh
#source ~/.bash_profile
SQL_PATH=/home/mam_jupyter/jupyter_dir/artefact/lorraine/sql
LOG_DIR=/home/mam_jupyter/jupyter_dir/artefact/lorraine/sql/logs

echo "---------------------  online ccm feature cleansing begin   -----------------------"
echo "---------------------step3/CCMpoint_sum.sql start......"
hive -f $SQL_PATH/CCMpoint_sum.sql >& $LOG_DIR/CCMpoint_sum.log
echo "---------------------step3/CCMpoint_sum.sql end ......"
echo "---------------------step3/CCMpoint_last_date.sql start......"
hive -f $SQL_PATH/CCMpoint_last_date.sql >& $LOG_DIR/CCMpoint_last_date.log
echo "---------------------step3/CCMpoint_last_date.sql end ......"
echo "---------------------step3/CCMpoint_sum_before.sql start......"
hive -f $SQL_PATH/CCMpoint_sum_before.sql >& $LOG_DIR/CCMpoint_sum_before.log
echo "---------------------step3/CCMpoint_sum_before.sql end ......"
echo "---------------------step3/CCMpoint_all_features.sql start......"
hive -f $SQL_PATH/CCMpoint_all_features.sql >& $LOG_DIR/CCMpoint_all_features.log
echo "---------------------step3/CCMpoint_all_features.sql end ......"
echo "---------------------  online ccm feature cleansing finish   -----------------------"





