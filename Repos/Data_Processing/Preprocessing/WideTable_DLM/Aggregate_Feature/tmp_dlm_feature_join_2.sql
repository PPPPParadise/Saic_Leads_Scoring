set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
set tez.queue.name=${queuename};
--
DROP TABLE IF EXISTS marketing_modeling.tmp_dlm_feature_join_2;
CREATE TABLE marketing_modeling.tmp_dlm_feature_join_2 as 
SELECT 
	a1.mobile,
	a2.dealf_succ_firvisit_diff,    --成交时间与成交经销商处首次到店时间日期差值
	a3.dealf_succ_lastvisit_diff,   --成交时间与成交经销商处最后一次到店时间日期差值
	a4.avg_fir_sec_visit_diff,      --平均首次到店时间与二次到店时间日期差值
	a5.fircard_firvisit_dtbt_diff,  --建卡时间距首次到店时间日期差值(每个经销商有个value)
	a5.avg_fircard_firvisit_diff,     --平均建卡时间距首次到店时间日期差值
	a6.avg_firleads_firvisit_diff,    --平均首次留资时间距首次到店时间日期差值
	a7.fir_activity_time,             -- 首次活动时间
	a7.activity_ttl,                   -- 总活动次数 
	if(a8.natural_visit is null,0,a8.natural_visit) as natural_visit,                  --自然到店
	a9.fir_leads_deal_diff_y,           -- 上次成交时间距首次留资时间年数
	if(a10.has_deliver_history is null,0,has_deliver_history) as   has_deliver_history         --之前是否荣威/名爵车主
FROM 
	marketing_modeling.tmp_dlm_mobile_mapping a1
left join 
	marketing_modeling.tmp_dlm_cust_activity_processing a7 on a1.mobile = a7.mobile
left join 
	marketing_modeling.tmp_dlm_dealf_succ_firvisit_diff a2 on a1.mobile = a2.mobile
left join 
	marketing_modeling.tmp_dlm_dealf_succ_lastvisit_diff a3 on a1.mobile = a3.mobile
left join 
	marketing_modeling.tmp_dlm_avg_fir_sec_visit_diff a4 on a1.mobile = a4.mobile
left join 
	marketing_modeling.tmp_dlm_avg_fircard_firvisit_diff a5 on a1.mobile = a5.mobile
left join 
	marketing_modeling.tmp_dlm_avg_firleads_firvisit_diff a6 on a1.mobile = a6.mobile
left join 
	marketing_modeling.tmp_dlm_natural_visit a8 on a1.mobile = a8.mobile
left join 
	marketing_modeling.tmp_dlm_fir_leads_deal_diff_y a9 on a1.mobile = a9.mobile
left join 
	marketing_modeling.tmp_dlm_has_deliver_history a10 on a1.mobile = a10.mobile
;