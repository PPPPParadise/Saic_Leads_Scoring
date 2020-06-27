#!/bin/bash
pt=$3
thres_upper=0.75
thres_mid=0.5
targeted_vol=10000
if [ `expr length $pt` == 8 ] && date -d $pt +%Y%m%d > /dev/null 2>&1; then
    echo $pt; 
else
    echo "pt=$pt 输入的日期格式不正确，应为yyyymmdd";
    exit 1; 
fi
hive -hivevar pt=$pt -hivevar thres_upper=$thres_upper -hivevar thres_mid=$thres_mid -hivevar targeted_vol=$targeted_vol -f SQL/mm_model_application_ingestion.sql