set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
set hive.mapjoin.smalltable.filesize=55000000;
set hive.auto.convert.join=false;  -- #取消小表加载至内存中
set tez.queue.name=${queuename};
set mapreduce.job.queuename=${queuename};

insert overwrite table marketing_modeling.app_sis_model_features partition(pt='${pt}')
select 
	a.mobile,
	a.brand,
	a.process_time,
	a.ob_result,
	b.fir_dealfail_d as fir_fail_date,
	b.last_dealfail_d as last_fail_date,
	c.first_card_time as fir_card_date,
	c.last_card_time as last_card_date,
	d.last_leads_time,
	e.last_visit_time,
	b.fail_time as fail_count,
	d.leads_count as leads_count,
	c.card_ttl as card_count,
	e.visit_ttl as visit_count,
	f.activity_ttl as activity_count,
	g.trail_ttl as trail_count,
	d.dealer_count as dealer_count,
	j.oppor_series_count as leads_model_count,
	i.is_failed_call as failed_called,
	h.age,
	h.city,
	h.sex,
	(d.first_resource_count+d.second_resource_count) as leads_source_count,
	d.first_resource_name as last_lead_first_source,
	d.second_resource_name as last_lead_second_source,
	c.lowest_cust_level
	
from
	marketing_modeling.fail_outbound a
left join
	marketing_modeling.ot_fail_features b
on 
	a.mobile = b.mobile and 
	a.process_time = b.process_time and
	a.brand = b.brand
left join 
	marketing_modeling.ot_cust_features c
on 
	a.mobile = c.mobile and 
	a.process_time = c.process_time and
	a.brand = c.brand
left join
	marketing_modeling.ot_leads_features d
on 
	a.mobile = d.mobile and 
	a.process_time = d.process_time and
	a.brand = d.brand
left join
	marketing_modeling.ot_visit_features e
on 
	a.mobile = e.mobile and 
	a.process_time = e.process_time and
	a.brand = e.brand
left join
	marketing_modeling.ot_activity_features f
on 
	a.mobile = f.mobile and 
	a.process_time = f.process_time and
	a.brand = f.brand
left join 
	marketing_modeling.ot_trail_features g
on 
	a.mobile = g.mobile and 
	a.process_time = g.process_time and
	a.brand = g.brand
left join 
	marketing_modeling.app_cdp_wide_info h
on 
	a.mobile = h.id
left join
	marketing_modeling.ot_is_failed_call i
on
	a.mobile = i.mobile
left join
    marketing_modeling.ot_oppor_features j
on 
	a.mobile = j.mobile and 
	a.process_time = j.process_time and
	a.brand = j.brand

group by
	a.mobile,
	a.brand,
	a.process_time,
	a.ob_result,
	b.fir_dealfail_d,
	b.last_dealfail_d,
	c.first_card_time,
	c.last_card_time,
	d.last_leads_time,
	e.last_visit_time,
	b.fail_time,
	c.card_ttl,
	d.leads_count,
	e.visit_ttl,
	f.activity_ttl,
	g.trail_ttl,
	d.dealer_count,
	j.oppor_series_count,
	i.is_failed_call,
	h.age,
	h.city,
	h.sex,
	d.first_resource_count,
	d.second_resource_count,
	d.second_resource_name,
	d.first_resource_name,
	c.lowest_cust_level
	