#!/bin/bash
pt=$3
if [ `expr length $pt` == 8 ] && date -d $pt +%Y%m%d > /dev/null 2>&1; then
    echo $pt; 
else
    echo "pt=$pt 输入的日期格式不正确，应为yyyymmdd";
    exit 1; 
fi
hive -hivevar pt=$pt -f SQL/mm_big_wide_info_col_used.sql

hadoop fs -cat hdfs://tcluster/warehouse/tablespace/managed/hive/marketing_modeling.db/app_model_input_data/*  >  app_model_input_data.txt
echo "=======================app_model_input_data.txt success"
sed -i 's/\\N/NULL/g' app_model_input_data.txt