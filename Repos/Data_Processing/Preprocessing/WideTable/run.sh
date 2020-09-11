#!/bin/bash
created_time=$(date "+%Y-%m-%d %H:%M:%S")
queuename=`awk -F '=' '/\[HIVE\]/{a=1}a==1&&$1~/job.queuename/{print $2;exit}' ../config.ini`
pt=$3
if [ `expr length $pt` == 8 ] && date -d $pt +%Y%m%d > /dev/null 2>&1; then
    echo $pt; 
else
	echo "pt=$pt 输入的日期格式不正确，应为yyyymmdd";
	exit 1; 
fi

#echo "=====================app_big_wide_info_tmp.sql start....................."
#hive -hivevar pt=$pt -hivevar queuename=$queuename -f sql/app_big_wide_info_tmp.sql
#if [ $? -ne 0 ];then 
#    echo "app_big_wide_info_tmp.sql 处理失败。code=$?";
#    exit 1; 
#else
#    echo "=====================app_big_wide_info_tmp....."
#fi

echo "=====================app_big_wide_info.sql start....................."
hive -hivevar pt=$pt -hivevar queuename=$queuename -f sql/app_big_wide_info.sql
if [ $? -ne 0 ];then 
    echo "app_big_wide_info.sql 处理失败。code=$?";
    exit 1; 
else
    echo "=====================app_big_wide_info....."
fi
app_name='app_big_wide_info'
hive_amount=$(hive -e "select count(1) from marketing_modeling.$app_name where pt=$pt")
real_amount=`echo $hive_amount | awk -F "|" '{print $4}'`
end_time=$(date "+%Y-%m-%d %H:%M:%S")
app_name='app_big_wide_info_proc'
echo "hive amount:" $hive_amount
echo $real_amount
echo $end_time
echo $app_name
mysql -h10.129.170.9 -uozmoni -pPassw0rd@ozmoni -e "INSERT INTO ooziemonitor.WF_JOBS_AMOUNT(app_name,created_time,end_time,real_amount) VALUES('$app_name','$created_time','$end_time','$real_amount')"
