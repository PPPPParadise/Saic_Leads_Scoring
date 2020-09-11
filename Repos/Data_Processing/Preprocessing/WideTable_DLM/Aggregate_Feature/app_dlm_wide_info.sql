set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
set tez.queue.name=${queuename};

-- 成交时间差计算
DROP TABLE IF EXISTS marketing_modeling.app_dlm_wide_info;
CREATE TABLE marketing_modeling.app_dlm_wide_info as 
SELECT 
	a1.*,
	a2.dealf_succ_firvisit_diff,    --成交时间与成交经销商处首次到店时间日期差值
	a2.dealf_succ_lastvisit_diff,   --成交时间与成交经销商处最后一次到店时间日期差值
	a2.avg_fir_sec_visit_diff,      --平均首次到店时间与二次到店时间日期差值
	a2.fircard_firvisit_dtbt_diff,  --建卡时间距首次到店时间日期差值(每个经销商有个value)
	a2.avg_fircard_firvisit_diff,     --平均建卡时间距首次到店时间日期差值
	a2.avg_firleads_firvisit_diff,    --平均首次留资时间距首次到店时间日期差值
	a2.fir_activity_time,             -- 首次活动时间
	a2.activity_ttl,                   -- 总活动次数
	a2.natural_visit,                  --自然到店
	a2.fir_leads_deal_diff_y,          -- 上次成交时间距首次留资时间年数	
	a2.has_deliver_history,         --之前是否荣威/名爵车主
	
	a3.trail_count_d30,           --过去30天总试驾数
	a3.trail_count_d90,            --过去30天总试驾数
	a3.visit_count_d15,            --过去15天总到店次数
	a3.visit_count_d30,            --过去30天总到店次数
	a3.visit_count_d90,             --过去90天总到店次数
	a3.followup_d7,            --过去7天跟进次数
	a3.followup_d15,           --过去15天跟进次数
	a3.followup_d30,           --过去30天跟进次数
	a3.followup_d60,           --过去60天跟进次数
	a3.followup_d90,            --过去90天跟进次数
	a3.activity_count_d15,     --过去15天参加活动次数
	a3.activity_count_d30,     --过去30天参加活动次数
	a3.activity_count_d60,     --过去60天参加活动次数
	a3.activity_count_d90,      --过去90天参加活动次数	
	
	a4.firlead_dealf_diff,        --- 首次留资时间距最后成交/战败时间差值
	a4.lastlead_dealf_diff,       --- 最后一次留资时间距最后成交/战败时间差值
	a4.last_dfail_dealf_diff,	  --- 客户最后一次战败日期距成交/战败日期天数
	a4.firfollow_dealf_diff,      -- 首次跟进日期距成交/战败时间差值
	a4.lastfollow_dealf_diff,	  --- 最后一次跟进日期距成交/战败日期差值
	a4.dealf_lastvisit_diff,	  -- 成交/战败时间与全经销商处最后一次到店时间日期差值
	a4.dealf_firvisit_diff,	      --- 成交/战败时间与全经销商处首次到店时间日期差值
	a4.lasttrail_dealf_diff,	  --- 最后一次试乘试驾日期距最后成交/战败时间差值
	a4.firleads_firvisit_diff,	  ---首次留资时间与首次到店时间日期差值
	a4.leads_dtbt_ppt,            -- 留资经销商到店比例
	a4.fircard_firvisit_diff,	  -- 首次建卡时间距首次到店时间日期差值
	a4.fir_dealfail_deal_diff,	  -- 客户首次战败日期距成交时间天数
	a4.deal_fail_ppt,	          ---战败比例
	a4.last_activity_dealf_diff,  --最后一次参加活动时间距成交(战败)日期差值
	a4.fir_activity_dealf_diff,    --首次参加活动时间距成交(战败)日期差值
	a4.fir_order_leads_diff,	--首次下定时间与首次留资时间的日期差值
	a4.fir_order_visit_diff,	--首次下定时间与首次到店时间的日期差值
	a4.fir_order_trail_diff,	    --首次下定时间与首次试乘试驾的日期差值
	a5.leads_pool_status
FROM
	marketing_modeling.tmp_dlm_feature_join_1 a1
left join 
	marketing_modeling.tmp_dlm_feature_join_2 a2 on a1.mobile = a2.mobile 
left join 
	marketing_modeling.tmp_dlm_feature_join_3 a3 on a1.mobile = a3.mobile
left join 
	marketing_modeling.tmp_dlm_feature_join_4 a4 on a1.mobile = a4.mobile
left join 
	marketing_modeling.tmp_leads_pool_status a5 on a1.mobile = a5.mobile
WHERE a1.mobile regexp "^[1][3-9][0-9]{9}$"
;