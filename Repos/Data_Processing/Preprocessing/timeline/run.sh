#!/bin/bash
timeline=.
#如果文件夹不存在，创建文件夹
if [ ! -d "logs/timeline" ]; then
  mkdir -p logs/timeline
fi
timeline_LOG=logs/timeline
starttime=`date +'%Y-%m-%d %H:%M:%S'`


echo "=================begin_time: "$starttime" ==================================="
echo "---------------------dmp_user_info.sql start_1......" 
hive -f $timeline/dmp_user_info.sql >& $timeline_LOG/dmp_user_info.log
#if [ $? -ne 0 ];then    echo exit; else    echo "leads_feature_cleansing.sql end ......"; fi
echo "---------------------gio_finance_count.sql start._2....."
hive -f $timeline/gio_finance_count.sql >& $timeline_LOG/finance_count.log &
echo "---------------------total_browse_time.sql start_1......" 
hive -f $timeline/total_browse_time.sql >& $timeline_LOG/total_browse_time.log
#if [ $? -ne 0 ];then    echo exit; else    echo "step1/cust_feature_cleansing.sql end ......"; fi
echo "---------------------timeline_behavior.sql start._2...return_code:"$?
hive -f $timeline/timeline_behavior.sql >& $timeline_LOG/timeline_behavior.log &
echo "---------------------lsh custbase end ......return_code:"$?
