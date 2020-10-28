#!/bin/bash
queuename=`awk -F '=' '/\[HIVE\]/{a=1}a==1&&$1~/job.queuename/{print $2;exit}' ../config.ini`
pt=$3
created_time=$(date "+%Y-%m-%d %H:%M:%S")
echo "===============================app_model_sis_base.sql"
hive -hivevar pt=$pt -hivevar queuename=$queuename -f app_sis_model_base.sql
if [ $? -ne 0 ];then 
    echo "app_model_sis_base.sql 处理失败。code=$?";
    exit 1; 
else
    echo "=====================app_model_sis_base....."
fi 
echo "===============================tmo_sis_model_ot_oppor_features.sql"
hive -hivevar pt=$pt -hivevar queuename=$queuename -f tmp_sis_model_ot_oppor_features.sql
if [ $? -ne 0 ];then 
    echo "tmo_sis_model_ot_oppor_features.sql 处理失败。code=$?";
    exit 1; 
else
    echo "=====================tmo_sis_model_ot_oppor_features....."
fi   

echo "===============================tmp_sis_model_trail_features.sql"
hive -hivevar pt=$pt -hivevar queuename=$queuename -f tmp_sis_model_trail_features.sql
if [ $? -ne 0 ];then
    echo "tmp_sis_model_trail_features.sql 处理失败。code=$?";
    exit 1;
else
    echo "=====================tmp_sis_model_trail_features.sql....."
fi

echo "===============================tmp_sis_model_fail_time.sql"
hive -hivevar pt=$pt -hivevar queuename=$queuename -f tmp_sis_model_fail_time.sql
if [ $? -ne 0 ];then 
    echo "tmp_sis_model_fail_time.sql 处理失败。code=$?";
    exit 1; 
else
    echo "=====================tmp_sis_model_fail_time....."
fi

echo "===============================tmp_sis_model_cust_features.sql"
hive -hivevar pt=$pt -hivevar queuename=$queuename -f tmp_sis_model_cust_features.sql
if [ $? -ne 0 ];then 
    echo "tmp_sis_model_cust_features.sql 处理失败。code=$?";
    exit 1; 
else
    echo "=====================tmp_sis_model_cust_features....."
fi


echo "===============================tmp_sis_model_visit_features.sql"
hive -hivevar pt=$pt -hivevar queuename=$queuename -f tmp_sis_model_visit_features.sql
if [ $? -ne 0 ];then 
    echo "tmp_sis_model_visit_features.sql 处理失败。code=$?";
    exit 1; 
else
    echo "=====================tmp_sis_model_visit_features....."
fi


echo "===============================tmp_sis_model_leads_source.sql"
hive -hivevar pt=$pt -hivevar queuename=$queuename -f tmp_sis_model_leads_source.sql
if [ $? -ne 0 ];then 
    echo "tmp_sis_model_leads_source.sql 处理失败。code=$?";
    exit 1; 
else
    echo "=====================tmp_sis_model_leads_source....."
fi


echo "===============================tmp_sis_model_follow_features.sql"
hive -hivevar pt=$pt -hivevar queuename=$queuename -f tmp_sis_model_follow_features.sql
if [ $? -ne 0 ];then 
    echo "tmp_sis_model_follow_features.sql 处理失败。code=$?";
    exit 1; 
else
    echo "=====================tmp_sis_model_follow_features....."
fi


echo "===============================tmp_sis_model_is_failed_call.sql"
hive -hivevar pt=$pt -hivevar queuename=$queuename -f tmp_sis_model_is_failed_call.sql
if [ $? -ne 0 ];then 
    echo "tmp_sis_model_is_failed_call.sql 处理失败。code=$?";
    exit 1; 
else
    echo "=====================tmp_sis_model_is_failed_call....."
fi


echo "===============================tmp_sis_model_activity_features.sql"
hive -hivevar pt=$pt -hivevar queuename=$queuename -f tmp_sis_model_activity_features.sql
if [ $? -ne 0 ];then 
    echo "tmp_sis_model_activity_features.sql 处理失败。code=$?";
    exit 1; 
else
    echo "=====================tmp_sis_model_activity_features....."
fi

echo "===============================app_sis_model_features.sql"
hive -hivevar pt=$pt -hivevar queuename=$queuename -f app_sis_model_features.sql
if [ $? -ne 0 ];then 
    echo "app_sis_model_features.sql 处理失败。code=$?";
    exit 1; 
else
    echo "=====================app_sis_model_features....."
fi
