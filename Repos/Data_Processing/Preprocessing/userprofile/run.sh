#!/bin/bash
queuename=`awk -F '=' '/\[HIVE\]/{a=1}a==1&&$1~/job.queuename/{print $2;exit}' ../config.ini`
pt=$3
created_time=$(date "+%Y-%m-%d %H:%M:%S")
if [ `expr length $pt` == 8 ] && date -d $pt +%Y%m%d > /dev/null 2>&1; then
    echo $pt; 
else    
    echo "pt=$pt 输入的日期格式不正确，应为yyyymmdd";
    exit 1; 
fi   
echo "===============================tmp_buying_rate.sql"
hive -hivevar pt=$pt -hivevar queuename=$queuename -f tmp_buying_rate.sql
if [ $? -ne 0 ];then 
    echo "tmp_buying_rate.sql 处理失败。code=$?";
    exit 1; 
else
    echo "=====================tmp_buying_rate....."
fi 
echo "===============================userprofile.sql"
hive -hivevar pt=$pt -hivevar queuename=$queuename -f userprofile.sql
if [ $? -ne 0 ];then 
    echo "userprofile.sql 处理失败。code=$?";
    exit 1; 
else
    echo "=====================userprofile....."
fi
app_name='edw_mkt_userprofile'
hive_amount=$(hive -e "select count(1) from marketing_modeling.$app_name where pt=$pt")
real_amount=`echo $hive_amount | awk -F "|" '{print $4}'`
end_time=$(date "+%Y-%m-%d %H:%M:%S")
app_name='edw_mkt_userprofile_proc'
echo "hive amount:" $hive_amount
echo $real_amount
echo $end_time
echo $app_name
mysql -h10.129.170.9 -uozmoni -pPassw0rd@ozmoni -e "INSERT INTO ooziemonitor.WF_JOBS_AMOUNT(app_name,created_time,end_time,real_amount) VALUES('$app_name','$created_time','$end_time','$real_amount')"
