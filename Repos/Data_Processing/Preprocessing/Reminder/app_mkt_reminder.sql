set tez.queue.name=${queuename};
INSERT OVERWRITE TABLE marketing_modeling.app_mkt_reminder partition (pt='${pt}')
SELECT 
	null as id,
	tt.mobile,
	tt.dealer_id as dealer_id,
	null as event_id,
	concat_ws(',', collect_set(concat (tt.event_id,'|', tt.event_content,'|', tt.attr1))) as event_content,
	from_unixtime((unix_timestamp()+8*60*60)) as create_time,
	null as attr1,
	null as attr2,
	null as attr3								 

FROM (
	SELECT 
		null AS id,
		aa.mobile as mobile,
		aa.dealer_id,
		3 event_id,
		'客户购车意向级别上升，请把握跟进节奏与跟进内容' event_content,
		current_timestamp() as create_time,
		'资料传播&电话沟通' as attr1,
		null as attr2,
		null as attr3
	FROM
	(
		SELECT
			a.mobile as mobile,
			b.dealer_id,
			row_number() over (PARTITION BY b.mobile ORDER BY b.ratio desc) as rn
		FROM
			marketing_modeling.tmp_user_intention_changing a
		LEFT JOIN
			marketing_modeling.tmp_dealer_info b
		ON 
			a.mobile = b.mobile
		WHERE
			b.dealer_id IS NOT NULL  and a.t > a.y
	) aa
	WHERE aa.rn = 1
	GROUP BY aa.mobile,aa.dealer_id
	UNION ALL
	SELECT 
		null AS id,
		bb.mobile as mobile,
		bb.dealer_id,
		2 event_id,
		'客户有高挽回可能性' event_content,
		current_timestamp() as create_time,
		'资料传播&电话沟通' as attr1,
		null as attr2,
		null as attr3
	FROM
	(
		SELECT 
			aa.mobile as mobile,
			aa.dealer_id
		FROM
		(
			SELECT
				a.mobile as mobile,
				b.dealer_id,
				row_number() over (PARTITION BY b.mobile ORDER BY b.ratio desc) as rn
			FROM
				marketing_modeling.tmp_profile_changing_alert a
			LEFT JOIN
				marketing_modeling.tmp_dealer_info b
			ON 
				a.mobile = b.mobile
			WHERE
				b.dealer_id IS NOT NULL  and a.to_golden_dealer=1
		) aa
		where aa.rn = 1
		GROUP BY aa.mobile,aa.dealer_id
		UNION ALL
		SELECT 
			aa.mobile as mobile,
			aa.dealer_id
		FROM
		(
			SELECT
				a.mobile as mobile,
				b.dealer_id as dealer_id
			FROM
				marketing_modeling.tmp_profile_changing_alert a
			LEFT JOIN
				marketing_modeling.tmp_dealer_info b
			ON 
				a.mobile = b.mobile
			WHERE
				b.dealer_id IS NOT NULL and a.to_delivered_dealer=1
			GROUP BY a.mobile,b.dealer_id
		) aa
		GROUP BY aa.mobile,aa.dealer_id
	 ) bb
	GROUP BY bb.mobile,bb.dealer_id
	UNION ALL
	SELECT 
		null AS id,
		bb.mobile as mobile,
		bb.dealer_id,
		1 event_id,
		'客户有流失可能性，请及时沟通' event_content,
		current_timestamp() as create_time,
		'资料传播&电话沟通' as attr1,
		null as attr2,
		null as attr3
	FROM
	(
		SELECT 
			aa.mobile as mobile,
			aa.dealer_id
		FROM
		(
			SELECT
				a.mobile as mobile,
				b.dealer_id,
				row_number() over (PARTITION BY b.mobile ORDER BY b.ratio desc) as rn
			FROM
				marketing_modeling.tmp_user_leaving_alert a,
				marketing_modeling.tmp_dealer_info b

			WHERE
				a.mobile = b.mobile and
				b.dealer_id IS NOT NULL  and 
				(a.wechat_unfollowed=1 or 
				a.intention_decrease=1 or 
				a.focus_diff_car=1 or 
				a.no_visiting=1 or
				a.stop_browsing = 1)
		) aa
		where aa.rn = 1
		GROUP BY aa.mobile,aa.dealer_id
		UNION ALL
		SELECT 
			aa.mobile as mobile,
			aa.dealer_id
		FROM
		(
			SELECT
				a.mobile as mobile,
				b.dealer_id as dealer_id
			FROM
				marketing_modeling.tmp_user_leaving_alert a
			LEFT JOIN
				marketing_modeling.tmp_dealer_info b
			ON 
				a.mobile = b.mobile
			WHERE
				b.dealer_id IS NOT NULL and 
				(a.no_follow_afr_visit = 1 or
				a.no_follow_afr_trail = 1 or
				a.no_follow_afr_3weeks = 1)
			GROUP BY a.mobile,b.dealer_id
		) aa
		GROUP BY aa.mobile,aa.dealer_id
	) bb
	GROUP BY bb.mobile,bb.dealer_id
) tt
GROUP BY
tt.mobile,tt.dealer_id
