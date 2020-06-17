#!bin/sh
#source ~/.bash_profile
SQL_PATH=/home/mam_jupyter/jupyter_dir/artefact/lorraine/sql
LOG_DIR=/home/mam_jupyter/jupyter_dir/artefact/lorraine/sql/logs
echo "---------------------  online growingio feature cleansing begin   -----------------------"
echo "---------------------step2/GIO_finance.sql start......"
hive -f $SQL_PATH/GIO_finance.sql >& $LOG_DIR/GIO_finance.log
echo "---------------------step2/GIO_finance.sql end ......"
echo "---------------------step2/GIO_MG_model_new.sql start......" 
hive -f $SQL_PATH/GIO_MG_model_new.sql >& $LOG_DIR/GIO_MG_model_new.log
echo "---------------------step2/GIO_MG_model_new.sql end ......"
echo "---------------------step2/GIO_MG_price_conf.sql start......" 
hive -f $SQL_PATH/GIO_MG_price_conf_new.sql >& $LOG_DIR/GIO_MG_price_conf_new.log
echo "---------------------step2/GIO_MG_price_conf.sql end ......"
echo "---------------------step2/GIO_MG_product_lightspot start......" 
hive -f $SQL_PATH/GIO_MG_product_lightspot.sql >& $LOG_DIR/GIO_MG_product_lightspot.log
echo "---------------------step2/GIO_MG_product_lightspot.sql end ......"
echo "---------------------step2/GIO_MG_360view.sql start......" 
hive -f $SQL_PATH/GIO_MG_360view.sql >& $LOG_DIR/GIO_MG_360view.log
echo "---------------------step2/GIO_MG_360view.sql end ......"
echo "---------------------step2/GIO_models_page.sql start......" 
hive -f $SQL_PATH/GIO_models_page.sql >& $LOG_DIR/GIO_models_page.log
echo "---------------------step2/GIO_models_page.sql end ......"
echo "---------------------step2/GIO_virtual_button.sql start......" 
hive -f $SQL_PATH/GIO_virtual_button.sql >& $LOG_DIR/GIO_virtual_button.log
echo "---------------------step2/GIO_virtual_button.sql end ......"
echo "---------------------step2/GIO_slideshow.sql start......" 
hive -f $SQL_PATH/GIO_slideshow.sql >& $LOG_DIR/GIO_slideshow.log
echo "---------------------step2/GIO_slideshow.sql end ......"
echo "---------------------step2/GIO_finding_dealer.sql start......" 
hive -f $SQL_PATH/GIO_finding_dealer.sql >& $LOG_DIR/GIO_finding_dealer.log
echo "---------------------step2/GIO_finding_dealer.sql end ......"
echo "---------------------step2/GIO_buying_online.sql start......" 
hive -f $SQL_PATH/GIO_buying_online.sql >& $LOG_DIR/GIO_buying_online.log
echo "---------------------step2/GIO_buying_online.sql end ......"
echo "---------------------step2/GIO_web_app_browsing_all.sql start......" 
hive -f $SQL_PATH/GIO_web_app_browsing_all.sql >& $LOG_DIR/GIO_web_app_browsing_all.log
echo "---------------------step2/GIO_web_app_browsing_all.sql end ......"
echo "---------------------step2/GIO_total_browse_time_new.sql start......" 
hive -f $SQL_PATH/GIO_total_browse_time_new.sql >& $LOG_DIR/GIO_total_browse_time_new.log
echo "---------------------step2/GIO_total_browse_time_new.sql end ......"
echo "---------------------step2/GIO_browse_d15.sql start......" 
hive -f $SQL_PATH/GIO_browse_d15.sql >& $LOG_DIR/GIO_browse_d15.log
echo "---------------------step2/GIO_browse_d15.sql end ......"
echo "---------------------step2/GIO_browse_d30.sql start......" 
hive -f $SQL_PATH/GIO_browse_d30.sql >& $LOG_DIR/GIO_browse_d30.log
echo "---------------------step2/GIO_browse_d30.sql end ......"
echo "---------------------step2/GIO_browse_d45.sql start......" 
hive -f $SQL_PATH/GIO_browse_d45.sql >& $LOG_DIR/GIO_browse_d45.log
echo "---------------------step2/GIO_browse_d45.sql end ......"
echo "---------------------step2/GIO_browse_avg.sql start......" 
hive -f $SQL_PATH/GIO_browse_avg.sql >& $LOG_DIR/GIO_browse_avg.log
echo "---------------------step2/GIO_browse_avg.sql end ......"
echo "---------------------step2/GIO_browse_diff.sql start......" 
hive -f $SQL_PATH/GIO_browse_diff.sql >& $LOG_DIR/GIO_browse_diff.log
echo "---------------------step2/GIO_browse_diff.sql end ......"
echo "---------------------step2/GIO_login.sql start......" 
hive -f $SQL_PATH/GIO_login.sql >& $LOG_DIR/GIO_login.log
echo "---------------------step2/GIO_login.sql end ......"
echo "---------------------step2/GIO_article.sql start......" 
hive -f $SQL_PATH/GIO_article.sql >& $LOG_DIR/GIO_article.log
echo "---------------------step2/GIO_article.sql end ......"





