#!bin/sh
#source ~/.bash_profile
SQL_PATH=/home/mam_jupyter/jupyter_dir/artefact/lorraine/sql
LOG_DIR=/home/mam_jupyter/jupyter_dir/artefact/lorraine/sql/logs

echo "---------------------  user mapping begin   -----------------------"
echo "---------------------step1/get_flag.sql start......"
hive -f $SQL_PATH/get_flag.sql >& $LOG_DIR/get_flag.log
echo "---------------------step1/get_flag.sql end ......"
echo "---------------------  user mapping end ---------------------------"




