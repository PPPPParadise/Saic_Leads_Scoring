-- 留资/建卡/到店/跟进用户合并
set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
set tez.queue.name=${queuename};

DROP TABLE IF EXISTS marketing_modeling.tmp_dlm_feature_join_1;
CREATE TABLE marketing_modeling.tmp_dlm_feature_join_1 as 
SELECT 
	a1.mobile,
	a.fir_leads_time,        -- 首次留资时间
	a.last_time as last_leads_time,       --  最后一次流资时间
	a.fir_sec_leads_diff,    -- 首次留资时间与二次留资时间日期差值
	a.leads_count,           -- 总留资次数
	a.leads_channel,         --留资渠道
	a.avg_leads_date,        -- 平均留资时间间隔
	a.leads_channel_count,   -- 留资渠道总数
	a.leads_dtbt_count,      -- 留资总经销商数
	a.leads_dtbt_coincide,   -- 留资经销商重合度
	a.leads_dtbt_level_1,    -- 一级经销商留资数量
	a.leads_dtbt_level_2,    -- 二级经销商留资数量
	b.cust_type,        --客户属性 (个人或企业)
	b.fir_card_time,    -- 首次建卡时间
	b.last_time as last_card_time,   -- 最后一次建卡时间
---	b.deal_flag,        -- 是否成交
	b.card_ttl,         -- 总建卡次数
	b.cust_ids,         -- 建卡ID数组
	b.clue_issued_times,        -- 线索总下发次数 
	c.fir_visit_time,       --	首次到店时间
	c.fir_sec_visit_diff,   ---	首次到店时间与二次到店时间日期差值
	c.visit_dtbt_count,     --	到店总经销商数量
	c.visit_ttl,            --	总到店次数
	c.avg_visit_date,       --平均到店时间间隔
	c.dealer_ids,           -- 经销商ID数组
	c.avg_visit_dtbt_count, --	平均每个经销商处到店次数  
	d.followup_ttl,         -- 总跟进次数
	e.fir_trail,            --- 首次试乘试驾时间
	e.last_reservation_time,   -- 最后一次试驾预约试驾
	e.trail_book_tll,       ------ 试乘试驾总预定次数
	e.trail_attend_ttl,     ---试乘试驾总参与次数
	e.trail_attend_ppt,      --试乘试驾参与率
	a2.leads_car_model_count,    --留资车型数量
	a2.leads_car_model_type,     --留资车型种类数
	a2.vel_series_ids,           --车型IDs
	a2.series_types,             --车型种类IDs
	a3.leads_car_model_count as rw_car_model_count,  --荣威留资车型数量
	a4.deal_time,                   -- 首次成交时间
	a4.last_time as last_deal_time,  -- 最后成交时间
	a4.deal_ttl,                    -- 总成交次数
	a4.deal_car_ttl,                -- 两年内总购买车辆数
	a5.fir_dealfail_d,               -- 首次战败时间
	a5.last_time as last_dealfail_d, -- 最后战败时间
	a6.is_deposit_order,             --是否下定
	a7.deal_flag                     -- 是否成交 1-成交，0-战败，null-跟进
 	
FROM
	marketing_modeling.tmp_dlm_mobile_mapping a1
	left join 
	marketing_modeling.tmp_dlm_leads_processing a on a1.mobile = a.mobile 
	left join 
	marketing_modeling.tmp_dlm_cust_processing b on a1.mobile = b.mobile 
    left join 
	marketing_modeling.tmp_dlm_hall_flow_processing c on a1.mobile = c.mobile
	left join 
	marketing_modeling.tmp_dlm_followup_processing d on a1.mobile = d.mobile 
	left join 
	marketing_modeling.tmp_dlm_receive_processing e on a1.mobile = e.mobile
	left join 
	(
		select * from marketing_modeling.tmp_dlm_oppor_processing
		where series_brand_id = 121
	)
	a2 on a1.mobile = a2.mobile 
	left join 
	(
		select * from marketing_modeling.tmp_dlm_oppor_processing
		where series_brand_id = 101
	)
	a3 on a1.mobile = a3.mobile 
	left join 
	marketing_modeling.tmp_dlm_deliver_processing a4 on a1.mobile = a4.mobile 
	left join	
	marketing_modeling.tmp_dlm_fail_processing a5 on a1.mobile = a5.mobile 
	left join	
	marketing_modeling.tmp_dlm_oppor_processing2 a6 on a1.mobile = a6.mobile
	left join	
	marketing_modeling.tmp_dlm_deal_flag_feature a7 on a1.mobile = a7.mobile
;
 

