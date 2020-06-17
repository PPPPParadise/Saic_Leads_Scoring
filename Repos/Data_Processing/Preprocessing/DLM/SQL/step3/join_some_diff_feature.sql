-- 成交时间差计算
DROP TABLE IF EXISTS marketing_modeling.dlm_tmp_join_some_diff;
CREATE TABLE marketing_modeling.dlm_tmp_join_some_diff as 
SELECT 
	a1.mobile,
	a2.firleads_firvisit_diff,	  ---首次留资时间与首次到店时间日期差值
	a3.leads_dtbt_ppt,            --留资经销商到店比例
	a4.fircard_firvisit_diff,	  --首次建卡时间距首次到店时间日期差值
	a5.fir_dealfail_deal_diff,	  -- 客户首次战败日期距成交时间天数
	a6.deal_fail_ppt	  ---战败比例
FROM
	marketing_modeling.dlm_tmp_mobile_mapping a1
left join 
(
	SELECT
		a.mobile, 
		datediff(b.fir_visit_time,a.fir_leads_time ) as firleads_firvisit_diff	  ---首次留资时间与首次到店时间日期差值
	FROM
		marketing_modeling.dlm_tmp_leads_processing a,
		marketing_modeling.dlm_tmp_hall_flow_processing b
	WHERE
		a.mobile = b.mobile and b.fir_visit_time > a.fir_leads_time
) a2 on a1.mobile = a2.mobile 
left join
(
	SELECT
		a.mobile, 
		round(b.visit_dtbt_count/a.leads_dtbt_count,2) as leads_dtbt_ppt     --留资经销商到店比例
	FROM
		marketing_modeling.dlm_tmp_leads_processing a,
		marketing_modeling.dlm_tmp_hall_flow_processing b
	WHERE
		a.mobile = b.mobile 
) a3 on a1.mobile = a3.mobile
left join
(
	SELECT
		a.mobile, 
		datediff(b.fir_visit_time,a.fir_card_time ) as fircard_firvisit_diff	  --首次建卡时间距首次到店时间日期差值
	FROM
		marketing_modeling.dlm_tmp_cust_processing a,
		marketing_modeling.dlm_tmp_hall_flow_processing b
	WHERE
		a.mobile = b.mobile and b.fir_visit_time > a.fir_card_time
) a4 on a1.mobile = a4.mobile 
left join 
(
	SELECT
		a.mobile, 
		datediff(b.deal_time,a.fir_dealfail_d) as fir_dealfail_deal_diff	  -- 客户首次战败日期距成交时间天数
	FROM
		marketing_modeling.dlm_tmp_fail_processing a,
		marketing_modeling.dlm_tmp_deliver_processing b
	WHERE
		a.mobile = b.mobile and b.deal_time > a.fir_dealfail_d
) a5 on a1.mobile = a5.mobile 
left join 
(
	SELECT
		a.mobile, 
		round(b.fail_cust_count/a.card_ttl,2) as deal_fail_ppt	  ---战败比例  ( 客户战败建卡数除以客户总建卡数)
	FROM
		marketing_modeling.dlm_tmp_cust_processing a,
		marketing_modeling.dlm_tmp_fail_processing b
	WHERE
		a.mobile = b.mobile 
) a6 on a1.mobile = a6.mobile 
;