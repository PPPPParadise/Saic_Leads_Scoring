SELECT count(mobile) from dtwarehouse.ods_dlm_t_cust_base GROUP BY mobile having count(mobile)>1;

-- 标记为删除的用户中有19241是成交状态的
select count(*),a.deal_flag from dtwarehouse.ods_dlm_t_cust_base a where a.deleted_flag = 'Y' GROUP BY a.deal_flag;
 
---- 首次建卡时间
SELECT
	a1.id as cust_id ,ai.mobile as mobile,a1.create_time
FROM
	(SELECT a.mobile, min(create_time) as create_time FROM dtwarehouse.ods_dlm_t_cust_base a GROUP BY a.mobile ) a2
LEFT JOIN dtwarehouse.ods_dlm_t_cust_base a1 ON a2.mobile = a1.mobile
AND a2.create_time = a1.create_time;
----- 成交 
select 
	a.id as cust_id,a.mobile,b.create_time
FROM 
	dtwarehouse.ods_dlm_t_cust_base a, dtwarehouse.ods_dlm_t_deliver_vel b
where 
	a.id = b.cust_id and a.deal_flag = 'Y';
 
------ 这个试乘表中的customer_id要和哪张表关联
 SELECT a.* from rdtwarehouse.ods_dlm_t_trial_receive a LIMIT 100;
 
 ---- 留资
 select count(*) from rdtwarehouse.ods_dlm_t_potential_customer_leads_m a where a.mobile is not null;
 ----
 
 
 