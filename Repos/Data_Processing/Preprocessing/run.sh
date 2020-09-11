#!/bin/bash
DLM=DLM/sql
#如果文件夹不存在，创建文件夹
if [ ! -d "logs/dlm" ]; then
  mkdir -p logs/dlm
fi
DLM_LOG=logs/dlm
starttime=`date +'%Y-%m-%d %H:%M:%S'`
pt=20200601
days=`awk -F '=' '/\[DLM\]/{a=1}a==1&&$1~/days/{print $2;exit}' config.ini`
queuename=bg=`awk -F '=' '/\[HIVE\]/{a=1}a==1&&$1~/job.queuename/{print $2;exit}' config.ini`
beginTime=`date -d "$days days ago" +%Y-%m-%d`
echo "=================begin_time: "$starttime" ==================================="
echo "=====================leads_feature_cleansing.sql start_1......" 
hive -hivevar pt=$pt -hivevar beginTime=$beginTime -hivevar queuename=$queuename -f $DLM/leads/leads_feature_cleansing.sql >& $DLM_LOG/leads_feature_cleansing.log
# if [ $? -ne 0 ];then    echo exit; else    echo "leads_feature_cleansing.sql end ......"; fi
echo "=====================leads_feature_processing.sql start._2....."
hive -hivevar queuename=$queuename -f $DLM/leads/leads_feature_processing.sql >& $DLM_LOG/leads_feature_processing.log &
echo "=====================lsh leads end ....return_code:"$?

echo "=====================sh custbase start_2...................." 
# 这个任务必须要执行完成后才能执行后面的任务
echo "=====================step1/cust_feature_cleansing.sql start_1......" 
hive -hivevar pt=$pt -hivevar beginTime=$beginTime -hivevar queuename=$queuename -f $DLM/custbase/cust_feature_cleansing.sql >& $DLM_LOG/cust_feature_cleansing.log
# if [ $? -ne 0 ];then    echo exit; else    echo "step1/cust_feature_cleansing.sql end ......"; fi
echo "=====================step1/cust_feature_processing.sql start._2...return_code:"$?
hive -hivevar queuename=$queuename -f $DLM/custbase/cust_feature_processing.sql >& $DLM_LOG/cust_feature_processing.log &
echo "=====================lsh custbase end ......return_code:"$?

echo "=====================sh hallflow start_3...................." 
echo "=====================hall_flow_feature_cleansing.sql start_1......" 
hive -hivevar pt=$pt -hivevar beginTime=$beginTime -hivevar queuename=$queuename -f $DLM/hallflow/hall_flow_feature_cleansing.sql >& $DLM_LOG/hall_flow_feature_cleansing.log
# if [ $? -ne 0 ];then    echo exit; else    echo "ok"; fi
echo "=====================hall_flow_feature_processing.sql start._2....return_code:"$?
hive -hivevar queuename=$queuename -f $DLM/hallflow/hall_flow_feature_processing.sql >& $DLM_LOG/hall_flow_feature_processing.log &
echo "=====================hall_flow_feature_processing.sql end ......"
echo "=====================lsh hallflow end ......return_code:"$?

echo "=====================sh trialReceive start_4...................." 
echo "=====================trial_receive_feature_cleansing.sql start_1......" 
hive -hivevar pt=$pt -hivevar beginTime=$beginTime -hivevar queuename=$queuename -f $DLM/trialReceive/trial_receive_feature_cleansing.sql >& $DLM_LOG/trial_receive_feature_cleansing.log
# if [ $? -ne 0 ];then    echo exit; else    echo "ok"; fi
echo "=====================trial_receive_feature_processing.sql start._2....."
hive -hivevar queuename=$queuename -f $DLM/trialReceive/trial_receive_feature_processing.sql >& $DLM_LOG/trial_receive_feature_processing.log &
echo "=====================lsh trialReceive end ....return_code:"$?

echo "=====================sh deliver start_5..................." 
echo "=====================deliver_feature_cleansing.sql start_1......" 
hive -hivevar pt=$pt -hivevar beginTime=$beginTime -hivevar queuename=$queuename -f $DLM/deliver/deliver_feature_cleansing.sql >& $DLM_LOG/deliver_feature_cleansing.log
# if [ $? -ne 0 ];then    echo exit; else    echo "ok"; fi
echo "=====================deliver_feature_cleansing.sql end ......"
echo "=====================deliver_feature_processing.sql start._2....."
hive -hivevar queuename=$queuename -f $DLM/deliver/deliver_feature_processing.sql >& $DLM_LOG/deliver_feature_processing.log &
echo "=====================lsh deliver end .....return_code:"$?

echo "=====================sh fail start_6...................."
echo "=====================fail_feature_cleansing.sql start_1......" 
hive -hivevar pt=$pt -hivevar beginTime=$beginTime -hivevar queuename=$queuename -f $DLM/fail/fail_feature_cleansing.sql >& $DLM_LOG/fail_feature_cleansing.log
#$ if [ $? -ne 0 ];then    echo exit; else    echo "ok"; fi
echo "=====================fail_feature_cleansing.sql end ......"
echo "=====================fail_feature_processing.sql start._2....."
hive -hivevar queuename=$queuename -f $DLM/fail/fail_feature_processing.sql >& $DLM_LOG/fail_feature_processing.log &
echo "=====================lsh fail end .....return_code:"$?

echo "=====================sh followup start_7....................." 
echo "=====================followup_feature_cleansing.sql start_1......" 
hive -hivevar pt=$pt -hivevar beginTime=$beginTime -hivevar queuename=$queuename -f $DLM/followup/followup_feature_cleansing.sql >& $DLM_LOG/followup_feature_cleansing.log
#$ if [ $? -ne 0 ];then    echo exit; else    echo "ok"; fi
echo "=====================followup_feature_processing.sql start._2....."
hive -hivevar queuename=$queuename -f $DLM/followup/followup_feature_processing.sql >& $DLM_LOG/followup_feature_processing.log &
echo "=====================lsh followup end ....return_code:"$?

echo "=====================sh custActivity start_8....................." 
hive -hivevar pt=$pt -hivevar beginTime=$beginTime -hivevar queuename=$queuename -f $DLM/custActivity/cust_activity_cleansing.sql >& $DLM_LOG/cust_activity_cleansing.log
#$ if [ $? -ne 0 ];then    echo exit; else    echo "ok"; fi
echo "=====================cust_activity_processing.sql start._2....."
hive -hivevar queuename=$queuename -f $DLM/custActivity/cust_activity_processing.sql >& $DLM_LOG/cust_activity_processing.log &
echo "=====================lsh custActivity end ....return_code:"$?

echo "=====================sh custVehicle start_9......................"
echo "=====================cust_vehicle_feature_cleansing.sql start_1......" 
hive -hivevar pt=$pt -hivevar beginTime=$beginTime -hivevar queuename=$queuename -f $DLM/custVehicle/cust_vehicle_feature_cleansing.sql >& $DLM_LOG/cust_vehicle_feature_cleansing.log
#if [ $? -ne 0 ];then    echo exit; else    echo "ok"; fi
echo "=====================lsh custVehicle end ....return_code:"$?

echo "=====================sh oppor start_10......................"
echo "=====================oppor_feature_cleansing.sql start_1......" 
hive -hivevar pt=$pt -hivevar beginTime=$beginTime -hivevar queuename=$queuename -f $DLM/oppor/oppor_feature_cleansing.sql >& $DLM_LOG/oppor_feature_cleansing.log
#if [ $? -ne 0 ];then    echo exit; else    echo "ok"; fi
echo "=====================oppor_feature_processing.sql start._2....."
hive -hivevar queuename=$queuename -f $DLM/oppor/oppor_feature_processing.sql >& $DLM_LOG/oppor_feature_processing.log
echo "=====================lsh oppor end ......return_code:"$?

echo "=====================sh step1 start_11....................."
echo "=====================step1 mobile_mapping_processing.sql start_1......" 
hive -hivevar queuename=$queuename -f $DLM/step1/mobile_mapping_processing.sql >& $DLM_LOG/mobile_mapping_processing.log
#if [ $? -ne 0 ];then    echo exit; else    echo "ok"; fi
echo "=====================step1/deal_flag_feature.sql start.2....."
hive -hivevar queuename=$queuename -f $DLM/step1/deal_flag_feature.sql >& $DLM_LOG/deal_flag_feature.log
echo "=====================step1/tmp_dlm_feature_join_1.sql start_3......" 
hive -hivevar queuename=$queuename -f $DLM/step1/tmp_dlm_feature_join_1.sql >& $DLM_LOG/tmp_dlm_feature_join_1.log 
echo "=====================step1/tmp_dlm_last_deal_time.sql start_4......" 
hive -hivevar queuename=$queuename -f $DLM/step1/tmp_dlm_last_deal_time.sql >& $DLM_LOG/tmp_dlm_last_deal_time.log &
echo "=====================lsh step1 end ......return_code:"$?

echo "=====================sh step2 start_12...................." 
echo "=====================step2/avg_firleads_firvisit_diff.sql start.1....." 
hive -hivevar queuename=$queuename -f $DLM/step2/avg_firleads_firvisit_diff.sql >& $DLM_LOG/avg_firleads_firvisit_diff.log

echo "=====================step2/avg_fircard_firvisit_diff.sql start.2....."
hive -hivevar queuename=$queuename -f $DLM/step2/avg_fircard_firvisit_diff.sql >& $DLM_LOG/avg_fircard_firvisit_diff.log &

echo "=====================step2/avg_fir_sec_visit_diff.sql start.3....."
hive -hivevar queuename=$queuename -f $DLM/step2/avg_fir_sec_visit_diff.sql >& $DLM_LOG/avg_fir_sec_visit_diff.log

echo "=====================step2/dealf_succ_firvisit_diff.sql start.4....."
hive -hivevar queuename=$queuename -f $DLM/step2/dealf_succ_firvisit_diff.sql >& $DLM_LOG/dealf_succ_firvisit_diff.log &

echo "=====================step2/dealf_succ_lastvisit_diff.sql start.5....."
hive -hivevar queuename=$queuename -f $DLM/step2/dealf_succ_lastvisit_diff.sql >& $DLM_LOG/dealf_succ_lastvisit_diff.log

echo "=====================step2/has_deliver_history.sql start.6....."
hive -hivevar queuename=$queuename -f $DLM/step2/has_deliver_history.sql >& $DLM_LOG/has_deliver_history.log &
echo "=====================lsh step2 end ......return_code:"$?

echo "=====================sh step3 start_13...................." 
echo "=====================step3/diff_dealf_firvisit.sql start.1....." 
hive -hivevar queuename=$queuename -f $DLM/step3/diff_dealf_firvisit.sql >& $DLM_LOG/diff_dealf_firvisit.log
echo "=====================step3/diff_dealf_firvisit.sql end ......"

echo "=====================step3/diff_fircard_dealf.sql start.2....."
hive -hivevar queuename=$queuename -f $DLM/step3/diff_fircard_dealf.sql >& $DLM_LOG/diff_fircard_dealf.log &

echo "=====================step3/diff_firfollow_dealf.sql start.3....."
hive -hivevar queuename=$queuename -f $DLM/step3/diff_firfollow_dealf.sql >& $DLM_LOG/diff_firfollow_dealf.log 

echo "=====================step3/diff_firstlead_dealf.sql start.4....."
hive -hivevar queuename=$queuename -f $DLM/step3/diff_firstlead_dealf.sql >& $DLM_LOG/diff_firstlead_dealf.log &

echo "=====================step3/diff_last_dfail_dealf.sql start.5....."
hive -hivevar queuename=$queuename -f $DLM/step3/diff_last_dfail_dealf.sql >& $DLM_LOG/diff_last_dfail_dealf.log 

echo "=====================step3/diff_lastlead_dealf.sql start.6....."
hive -hivevar queuename=$queuename -f $DLM/step3/diff_lastlead_dealf.sql >& $DLM_LOG/diff_lastlead_dealf.log &

echo "=====================step3/diff_lasttrail_dealf.sql start.7....."
hive -hivevar queuename=$queuename -f $DLM/step3/diff_lasttrail_dealf.sql >& $DLM_LOG/diff_lasttrail_dealf.log 

echo "=====================step3/join_some_diff_feature.sql start.8....."
hive -hivevar queuename=$queuename -f $DLM/step3/join_some_diff_feature.sql >& $DLM_LOG/join_some_diff_feature.log &

echo "=====================step3/fir_order_leads_diff.sql start.9....."
hive -hivevar queuename=$queuename -f $DLM/step3/fir_order_leads_diff.sql >& $DLM_LOG/fir_order_leads_diff.log 

echo "=====================step3/fir_order_visit_diff.sql start.10....."
hive -hivevar queuename=$queuename -f $DLM/step3/fir_order_visit_diff.sql >& $DLM_LOG/fir_order_visit_diff.log 

echo "=====================step3/fir_order_trail_diff.sql start.11....."
hive -hivevar queuename=$queuename -f $DLM/step3/fir_order_trail_diff.sql >& $DLM_LOG/fir_order_trail_diff.log &

echo "=====================step3/fir_activity_dealf_diff.sql start.12....."
hive -hivevar queuename=$queuename -f $DLM/step3/fir_activity_dealf_diff.sql >& $DLM_LOG/fir_activity_dealf_diff.log 

echo "=====================step3/last_activity_dealf_diff.sql start.13....."
hive -hivevar queuename=$queuename -f $DLM/step3/last_activity_dealf_diff.sql >& $DLM_LOG/last_activity_dealf_diff.log &

echo "=====================step3/fir_lead_dealf_diff_y.sql start.14....."
hive -hivevar queuename=$queuename -f $DLM/step3/fir_lead_dealf_diff_y.sql >& $DLM_LOG/fir_lead_dealf_diff_y.log 

echo "=====================step3/natural_visit.sql start.15....."
hive -hivevar queuename=$queuename -f $DLM/step3/natural_visit.sql >& $DLM_LOG/natural_visit.log &
echo "=====================lsh step3 end .....return_code:"$?

echo "=====================sh step3 start_14....................." 
echo "=====================step3/followup_d7.sql start.1....." 
hive -hivevar queuename=$queuename -f $DLM/step3/followup_d7.sql >& $DLM_LOG/followup_d7.log

echo "=====================step3/followup_d15.sql start.2....."
hive -hivevar queuename=$queuename -f $DLM/step3/followup_d15.sql >& $DLM_LOG/followup_d15.log &

echo "=====================step3/followup_d30.sql start.3....."
hive -hivevar queuename=$queuename -f $DLM/step3/followup_d30.sql >& $DLM_LOG/followup_d30.log

echo "=====================step3/followup_d60.sql start.4....."
hive -hivevar queuename=$queuename -f $DLM/step3/followup_d60.sql >& $DLM_LOG/followup_d60.log &

echo "=====================step3/followup_d90.sql start.5....."
hive -hivevar queuename=$queuename -f $DLM/step3/followup_d90.sql >& $DLM_LOG/followup_d90.log

echo "=====================step3/trail_count_d30.sql start.6....."
hive -hivevar queuename=$queuename -f $DLM/step3/trail_count_d30.sql >& $DLM_LOG/trail_count_d30.log &

echo "=====================step3/trail_count_d90.sql start.7....."
hive -hivevar queuename=$queuename -f $DLM/step3/trail_count_d90.sql >& $DLM_LOG/trail_count_d90.log

echo "=====================step3/visit_count_d15.sql start.8....."
hive -hivevar queuename=$queuename -f $DLM/step3/visit_count_d15.sql >& $DLM_LOG/visit_count_d15.log

echo "=====================step3/visit_count_d30.sql start.9....."
hive -hivevar queuename=$queuename -f $DLM/step3/visit_count_d30.sql >& $DLM_LOG/visit_count_d30.log &

echo "=====================step3/visit_count_d90.sql start.10....."
hive -hivevar queuename=$queuename -f $DLM/step3/visit_count_d90.sql >& $DLM_LOG/visit_count_d90.log

echo "=====================step3/activity_count_d15.sql start.11....."
hive -hivevar queuename=$queuename -f $DLM/step3/activity_count_d15.sql >& $DLM_LOG/activity_count_d15.log &

echo "=====================step3/activity_count_d30.sql start.12....."
hive -hivevar queuename=$queuename -f $DLM/step3/activity_count_d30.sql >& $DLM_LOG/activity_count_d30.log

echo "=====================step3/activity_count_d60.sql start.13....."
hive -hivevar queuename=$queuename -f $DLM/step3/activity_count_d60.sql >& $DLM_LOG/activity_count_d60.log  &

echo "=====================step3/activity_count_d90.sql start.14....."
hive -hivevar queuename=$queuename -f $DLM/step3/activity_count_d90.sql >& $DLM_LOG/activity_count_d90.log

echo "=====================lsh step3 end .....return_code:"$?

echo "=====================sh step3 start_15....................." 
echo "=====================join/tmp_dlm_feature_join_2.sql start._2....."
hive -hivevar queuename=$queuename -f $DLM/join/tmp_dlm_feature_join_2.sql >& $DLM_LOG/tmp_dlm_feature_join_2.log & 

echo "=====================join/tmp_dlm_feature_join_3.sql start_3......"
hive -hivevar queuename=$queuename -f $DLM/join/tmp_dlm_feature_join_3.sql >& $DLM_LOG/tmp_dlm_feature_join_3.log 

echo "=====================join/tmp_dlm_feature_join_4.sql start._4....."
hive -hivevar queuename=$queuename -f $DLM/join/tmp_dlm_feature_join_4.sql >& $DLM_LOG/tmp_dlm_feature_join_4.log 

echo "=====================join/mm_dlm_behavior_wide_info.sql start.._5...."
hive -hivevar queuename=$queuename -f $DLM/join/mm_dlm_behavior_wide_info.sql >& $DLM_LOG/mm_dlm_behavior_wide_info.log 
echo "=====================lsh step3 end .....return_code:"$?

endtime=`date +'%Y-%m-%d %H:%M:%S'`
echo "=================end_time: "$endtime" ==================================="

start_seconds=$(date --date="$starttime" +%s);
end_seconds=$(date --date="$endtime" +%s);
echo "本次运行时间： "$((end_seconds-start_seconds))"s"


