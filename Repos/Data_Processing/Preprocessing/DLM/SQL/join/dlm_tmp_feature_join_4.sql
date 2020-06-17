set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
--
DROP TABLE IF EXISTS marketing_modeling.dlm_tmp_feature_join_4;
CREATE TABLE marketing_modeling.dlm_tmp_feature_join_4 as 
SELECT 
	a1.mobile,
	a2.firlead_dealf_diff,        --- 首次留资时间距最后成交/战败时间差值
	a3.lastlead_dealf_diff,       --- 最后一次留资时间距最后成交/战败时间差值
	a4.last_dfail_dealf_diff,	  --- 客户最后一次战败日期距成交/战败日期天数
	a5.firfollow_dealf_diff,      -- 首次跟进日期距成交/战败时间差值
	a5.lastfollow_dealf_diff,	  --- 最后一次跟进日期距成交/战败日期差值
	a6.dealf_lastvisit_diff,	  -- 成交/战败时间与全经销商处最后一次到店时间日期差值
	a6.dealf_firvisit_diff,	      --- 成交/战败时间与全经销商处首次到店时间日期差值
	a7.lasttrail_dealf_diff,	  --- 最后一次试乘试驾日期距最后成交/战败时间差值
	a8.firleads_firvisit_diff,	  ---首次留资时间与首次到店时间日期差值
	a8.leads_dtbt_ppt,            -- 留资经销商到店比例
	a8.fircard_firvisit_diff,	  -- 首次建卡时间距首次到店时间日期差值
	a8.fir_dealfail_deal_diff,	  -- 客户首次战败日期距成交时间天数
	a8.deal_fail_ppt,	          ---战败比例
	a9.last_activity_dealf_diff,  --最后一次参加活动时间距成交(战败)日期差值
	a10.fir_activity_dealf_diff,   --首次参加活动时间距成交(战败)日期差值
	a11.fir_order_leads_diff,	--首次下定时间与首次留资时间的日期差值
	a12.fir_order_visit_diff,	--首次下定时间与首次到店时间的日期差值
	a13.fir_order_trail_diff	    --首次下定时间与首次试乘试驾的日期差值
FROM
	marketing_modeling.dlm_tmp_mobile_mapping a1
left join marketing_modeling.dlm_tmp_firlead_dealf_diff a2 on a1.mobile = a2.mobile
left join marketing_modeling.dlm_tmp_lastlead_dealf_diff a3 on a1.mobile = a3.mobile
left join marketing_modeling.dlm_tmp_last_dfail_dealf_diff a4 on a1.mobile = a4.mobile 
left join marketing_modeling.dlm_tmp_firfollow_dealf_diff a5 on a1.mobile = a5.mobile 
left join marketing_modeling.dlm_tmp_dealf_firvisit_diff a6 on a1.mobile = a6.mobile 
left join marketing_modeling.dlm_tmp_lasttrail_dealf_diff a7 on a1.mobile = a7.mobile
left join marketing_modeling.dlm_tmp_join_some_diff a8 on a1.mobile = a8.mobile
left join marketing_modeling.dlm_tmp_last_activity_dealf_diff a9 on a1.mobile = a9.mobile
left join marketing_modeling.dlm_tmp_fir_activity_dealf_diff a10 on a1.mobile = a10.mobile
left join marketing_modeling.dlm_tmp_fir_order_leads_diff a11 on a1.mobile = a11.mobile
left join marketing_modeling.dlm_tmp_fir_order_visit_diff a12 on a1.mobile = a12.mobile
left join marketing_modeling.dlm_tmp_fir_order_trail_diff a13 on a1.mobile = a13.mobile
;