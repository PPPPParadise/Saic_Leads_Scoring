#!bin/sh
#source ~/.bash_profile
SQL_PATH=/home/mam_jupyter/jupyter_dir/artefact/lorraine/sql
LOG_DIR=/home/mam_jupyter/jupyter_dir/artefact/lorraine/sq1/logs

echo "---------------------  online growingio feature processing begin   -----------------------"
echo "---------------------step2/GIO_all_features.sql start......"
hive -f $SQL_PATH/GIO_all_features.sql >& $LOG_DIR/GIO_all_features.log
echo "---------------------step2/GIO_all_features.sql end ......"





