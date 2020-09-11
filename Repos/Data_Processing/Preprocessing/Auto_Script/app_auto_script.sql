set tez.queue.name=${queuename};
INSERT OVERWRITE TABLE marketing_modeling.app_mkt_auto_script partition (pt='${pt}')
SELECT 	null as id,
		tt.mobile,
		-1 as dealer_id,
		concat_ws(',', collect_set(tt.auto_script_id)) as auto_script_id,
		from_unixtime((unix_timestamp()+8*60*60)) as create_time,
		1 as orders,
		null attr1,
		null attr2,
		null attr3

 FROM (
SELECT
	mobile,
	ids as auto_script_id
FROM
	marketing_modeling.tmp_app_script_step2
LATERAL VIEW explode(split(auto_script_id,',')) tt as ids 
where auto_script_id is not null
GROUP BY
mobile, ids

UNION ALL
SELECT

	mobile,
	case when calling_count is not null then '26'
		 when d_trail_attend_ttl is not null then '25'
		 when c_visit_count is not null then '24'
		 when s_first_call_result is not null then '23'
		 end as auto_script_id

FROM
	marketing_modeling.tmp_followup_auto_script
	
UNION ALL
SELECT
	mobile,
	cast(auto_script_id as string) as auto_script_id
FROM
	marketing_modeling.tmp_fail_reason_script
) tt
GROUP BY
tt.mobile
	
	