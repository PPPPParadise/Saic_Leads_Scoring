set tez.queue.name=${queuename};
DROP TABLE IF EXISTS marketing_modeling.tmp_dealer_info;
CREATE TABLE IF NOT EXISTS marketing_modeling.tmp_dealer_info AS
WITH deal AS(
	SELECT dealer_id,  count(1) as deal
	FROM dtwarehouse.ods_dlm_t_cust_base 
	WHERE pt='${pt}' and deal_flag='Y'
	GROUP BY dealer_id
),
deliver_sum AS (
	SELECT dealer_id, count(1) as deliver_num 
	FROM dtwarehouse.ods_dlm_t_potential_customer_leads_m
	WHERE pt='${pt}'
	GROUP BY dealer_id
),
dealer AS(
	SELECT 	b.dealer_id, 
			a.deal/b.deliver_num as ratio
	FROM 	deliver_sum b
	LEFT JOIN deal a
	ON a.dealer_id = b.dealer_id
)

SELECT a.mobile,cast(a.dealer_id as string) as dealer_id, b.ratio
FROM 
	(SELECT mobile,dealer_id 
		FROM dtwarehouse.ods_dlm_t_potential_customer_leads_m 
		WHERE pt='${pt}' and mobile regexp "^[1][3-9][0-9]{9}$" 
		GROUP BY mobile, dealer_id) a
LEFT JOIN dealer b
ON a.dealer_id = b.dealer_id
WHERE a.dealer_id IS NOT NULL