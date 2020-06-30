set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
set tez.queue.name=${queuename};

-- join 过去多少天xx次数的特征
DROP TABLE IF EXISTS marketing_modeling.tmp_dlm_feature_join_3;
CREATE TABLE marketing_modeling.tmp_dlm_feature_join_3 as 
SELECT 
	a1.mobile,
	a2.trail_count_d30,           --过去30天总试驾数
	a3.trail_count_d90,            --过去30天总试驾数
	a4.visit_count_d15,            --过去15天总到店次数
	a5.visit_count_d30,            --过去30天总到店次数
	a6.visit_count_d90,            --过去90天总到店次数
	a7.followup_d7,           --过去7天跟进次数
	a8.followup_d15,          --过去15天跟进次数
	a9.followup_d30,          --过去30天跟进次数
	a10.followup_d60,         --过去60天跟进次数
	a11.followup_d90,         --过去90天跟进次数
	a12.activity_count_d15,  --过去15天参加活动次数
	a13.activity_count_d30,  --过去30天参加活动次数
	a14.activity_count_d60,  --过去60天参加活动次数
	a15.activity_count_d90   --过去90天参加活动次数
FROM
	marketing_modeling.tmp_dlm_mobile_mapping a1
left join marketing_modeling.tmp_dlm_trail_count_d30 a2 on a1.mobile = a2.mobile
left join marketing_modeling.tmp_dlm_trail_count_d90 a3 on a1.mobile = a3.mobile
left join marketing_modeling.tmp_dlm_visit_count_d15 a4 on a1.mobile = a4.mobile 
left join marketing_modeling.tmp_dlm_visit_count_d30 a5 on a1.mobile = a5.mobile
left join marketing_modeling.tmp_dlm_visit_count_d90 a6 on a1.mobile = a6.mobile
left join marketing_modeling.tmp_dlm_followup_d7 a7 on a1.mobile = a7.mobile 
left join marketing_modeling.tmp_dlm_followup_d15 a8 on a1.mobile = a8.mobile 
left join marketing_modeling.tmp_dlm_followup_d30 a9 on a1.mobile = a9.mobile 
left join marketing_modeling.tmp_dlm_followup_d60 a10 on a1.mobile = a10.mobile
left join marketing_modeling.tmp_dlm_followup_d90 a11 on a1.mobile = a11.mobile
left join marketing_modeling.tmp_dlm_activity_count_d15 a12 on a1.mobile = a12.mobile
left join marketing_modeling.tmp_dlm_activity_count_d30 a13 on a1.mobile = a13.mobile
left join marketing_modeling.tmp_dlm_activity_count_d60 a14 on a1.mobile = a14.mobile
left join marketing_modeling.tmp_dlm_activity_count_d90 a15 on a1.mobile = a15.mobile
;