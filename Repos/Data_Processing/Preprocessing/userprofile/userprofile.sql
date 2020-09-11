SET mapreduce.map.memory.mb=4096;
SET mapreduce.reduce.memory.mb = 8192;
set tez.queue.name=${queuename};

INSERT OVERWRITE TABLE marketing_modeling.edw_mkt_userprofile partition(pt='${pt}')
SELECT  a.phone                                                                  AS mobile 
       ,a.c_sex                                                                  AS sex 
       ,a.c_city                                                                 AS city 
       ,a.c_age                                                                  AS age 
       ,a.h_models                                                               AS focus_brand 
       ,a.h_level                                                                AS fucus_model 
       ,a.h_have_car                                                             AS have_car 
       ,CASE WHEN a.d_fir_visit_time is not null THEN 1 ELSE 0 END               AS visited 
       ,a.d_trail_book_tll                                                       AS trail_booking_ttl 
       ,a.c_last_trail_time                                                      AS last_trail_time 
       ,a.d_leads_car_model_count                                                AS car_model_ttl 
       ,CASE WHEN (a.d_followup_d30 = 0 or a.d_followup_d30 is null) AND a.d_deal_flag < 1 THEN 1 ELSE 0 END AS no_deal 
       ,a.h_goal                                                                 AS buying_goal 
       ,a.h_loan                                                                 AS loan_ppt 
       ,a.h_priority_car_type                                                    AS car_type 
       ,a.h_priority_config_prefer                                               AS config_prefer 
       ,a.h_priority_change_car_and_buy                                          AS change_and_buy_ppt 
       ,a.h_models                                                               AS focus_models 
       ,a.h_budget                                                               AS budget 
       ,a.h_model_nums                                                           AS model_nums 
--        ,b.media                                                                  AS media_source 
--       ,b.creative                                                               AS media_content 
	   ,c.prob_tag																 AS prob
	   ,c.outbound																 AS outbound
	   ,a.d_trail_attend_ttl													 AS trail_attend_ttl
	   ,a.d_visit_ttl															 AS visit_ttl
	   ,b.buying_rate															 AS buying_rate
	   ,a.device_id																 AS device_id
	   ,a.union_id																 AS union_id
	   ,a.cookies																 AS cookies
	   ,d.fail_reason_fir_level													 AS fail_reason_fir_level
	   ,d.fail_reason_sec_level													 AS fail_reason_sec_level
FROM 
	(
		select b.phone,b.device_id,b.cookies,b.union_id,aa.*,
			row_number() over (partition by aa.mobile order by aa.d_fir_leads_time desc) as num 
		from (select * from marketing_modeling.app_big_wide_info where pt='${pt}') aa
		join
			 (select * from cdp.edw_cdp_id_mapping_overview where pt='${pt}' and phone is not null and phone != '') b
		on aa.mobile = b.phone 
				
	   ) a
LEFT JOIN marketing_modeling.tmp_buying_rate b
 ON a.mobile = b.mobile 
LEFT JOIN (select * from marketing_modeling.app_model_application where pt='${pt}') c
ON a.mobile = c.mobile
LEFT JOIN marketing_modeling.app_failed_reason d
ON a.mobile = d.mobile
where a.num = 1
;