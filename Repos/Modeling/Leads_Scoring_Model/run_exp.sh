#!/bin/bash
queuename=malg
pt=$3
created_time=$(date "+%Y-%m-%d %H:%M:%S")
if [ `expr length $pt` == 8 ] && date -d $pt +%Y%m%d > /dev/null 2>&1; then
    echo $pt; 
else
    echo "pt=$pt 输入的日期格式不正确，应为yyyymmdd";
    exit 1; 
fi
hive -hivevar pt=$pt -hivevar queuename=$queuename -f SQL/mm_big_wide_info_col_used.sql

#hadoop fs -cat hdfs://tcluster/warehouse/tablespace/managed/hive/marketing_modeling.db/app_model_input_data/*  >  app_model_input_data.txt
hadoop fs -cat hdfs://smpvprdCluster/warehouse/tablespace/managed/hive/marketing_modeling.db/app_model_input_data/pt=$pt/*  >  app_model_input_data.txt
echo "=======================app_model_input_data.txt success"
sed -i 's/\\N/NULL/g' app_model_input_data.txt
if [ $? -ne 0 ];then 
    echo "tmp_dlm_feature_join_3.sql 处理失败。code=$?";
    exit 1; 
else
    echo "=====================app_big_wide_info....."
fi
echo "=====================================insert mysql....."
app_name='app_model_input_data'
hive_amount=$(hive -e "select count(1) from marketing_modeling.$app_name where pt=$pt")
real_amount=`echo $hive_amount | awk -F "|" '{print $4}'`
end_time=$(date "+%Y-%m-%d %H:%M:%S")
app_name='app_model_input_data_proc'
echo "hive amount:" $hive_amount
echo $real_amount
echo $end_time
echo $app_name
mysql -h10.129.170.9 -uozmoni -pPassw0rd@ozmoni -e "INSERT INTO ooziemonitor.WF_JOBS_AMOUNT(app_name,created_time,end_time,real_amount) VALUES('$app_name','$created_time','$end_time','$real_amount')"
