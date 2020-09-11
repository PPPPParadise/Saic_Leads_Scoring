#!/bin/bash
pt=$3
queuename=malg
thres_upper=0.75
thres_mid=0.5
targeted_vol=10000
created_time=$(date "+%Y-%m-%d %H:%M:%S")
if [ `expr length $pt` == 8 ] && date -d $pt +%Y%m%d > /dev/null 2>&1; then
    echo $pt; 
else
    echo "pt=$pt 输入的日期格式不正确，应为yyyymmdd";
    exit 1; 
fi
hive -hivevar pt=$pt -hivevar queuename=$queuename -f SQL/insert_app_model_result.sql
if [ $? -ne 0 ];then 
    echo "tmp_dlm_feature_join_3.sql 处理失败。code=$?";
    exit 1; 
else
    echo "=====================app_big_wide_info....."
fi
echo "=====================================insert mysql....."
app_name='app_model_result'
hive_amount=$(hive -e "select count(1) from marketing_modeling.$app_name where pt=$pt")
real_amount=`echo $hive_amount | awk -F "|" '{print $4}'`
end_time=$(date "+%Y-%m-%d %H:%M:%S")
app_name='app_model_result_proc'
echo "hive amount:" $hive_amount
echo $real_amount
echo $end_time
echo $app_name
mysql -h10.129.170.9 -uozmoni -pPassw0rd@ozmoni -e "INSERT INTO ooziemonitor.WF_JOBS_AMOUNT(app_name,created_time,end_time,real_amount) VALUES('$app_name','$created_time','$end_time','$real_amount')"


created_time=$(date "+%Y-%m-%d %H:%M:%S")
echo "=====================================mm_model_application_ingestion start....."
hive -hivevar queuename=$queuename -hivevar pt=$pt -hivevar thres_upper=$thres_upper -hivevar thres_mid=$thres_mid -hivevar targeted_vol=$targeted_vol -f SQL/mm_model_application_ingestion.sql
if [ $? -ne 0 ];then 
    echo "tmp_dlm_feature_join_3.sql 处理失败。code=$?";
    exit 1; 
else
    echo "=====================app_big_wide_info....."
fi
echo "=====================================insert mysql....."
app_name='app_model_application'
hive_amount=$(hive -e "select count(1) from marketing_modeling.$app_name where pt=$pt")
real_amount=`echo $hive_amount | awk -F "|" '{print $4}'`
end_time=$(date "+%Y-%m-%d %H:%M:%S")
app_name='app_model_application_proc'
echo "hive amount:" $hive_amount
echo $real_amount
echo $end_time
echo $app_name
mysql -h10.129.170.9 -uozmoni -pPassw0rd@ozmoni -e "INSERT INTO ooziemonitor.WF_JOBS_AMOUNT(app_name,created_time,end_time,real_amount) VALUES('$app_name','$created_time','$end_time','$real_amount')"
