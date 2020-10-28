set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
set hive.mapjoin.smalltable.filesize=55000000;
set hive.auto.convert.join=false;  -- #取消小表加载至内存中
set tez.queue.name=${queuename};

--外呼手机号，时间，结果，品牌
drop table if exists marketing_modeling.fail_outbound;
create table marketing_modeling.fail_outbound as

select
	aa.mobile,
	aa.process_time,
	max(ob_result) as ob_result,
	brand,
	RANK() OVER(PARTITION BY mobile,brand ORDER BY process_time DESC) AS call_rank
from (
    select 
    	mobile,
    	substr(call_time, 1, 10) as process_time,
    	case when cust_level in ('A','B') then 1 else 0 end as ob_result,
    	'MG' as brand
    from  marketing_modeling.ods_manual_cultivation_call_info_mas 
    where 
        call_time is not null and 
        call_time != '' and
		activity_name like '%战败%'
		
    union
    SELECT 
        mobile_no AS mobile, 
		substr(d.created_time,1,10) AS processs_time,
        case when  d.ob_result_code = '007' then 1 else 0 end as ob_result,
        CASE WHEN c.brand like '%MG%' or c.brand like '%名爵%' THEN 'MG' ELSE 'RW' END AS brand
    FROM 
        (select * from dtwarehouse.ods_smcsc_tsk_customer where pt = '${pt}') a
    LEFT JOIN 
        (select * from dtwarehouse.ods_smcsc_tsk_task_item where pt = '${pt}') b
    ON a.id = b.cust_id
    LEFT JOIN 
        (select * from dtwarehouse.ods_smcsc_tsk_task_item_ext where pt = '${pt}') c
    ON b.id = c.task_item_id
    LEFT JOIN 
        (select * from dtwarehouse.ods_smcsc_tsk_task_item_process where pt = '${pt}') d
    ON b.id = d.task_item_id
    LEFT JOIN 
        (select * from dtwarehouse.ods_smcsc_tsk_task where pt = '${pt}') e
    ON b.task_id = e.id    
    WHERE mobile_no REGEXP "^[1][3-9][0-9]{9}$"
    AND d.created_time is not null
	AND task_type in ('003','004')
    union
    select
    	client_number as mobile,
    	substr(create_time,1,10) as process_time,
    	case when dealer_number is not null or dealer_number != '' then 1 else 0 end as ob_result,
    	case when client_brands IN ('MG', '名爵') THEN 'MG' ELSE 'RW' END AS brand	
    from
    	marketing_modeling.app_manual_sis_info
	where 
		second_source like '%战败%'
) aa
where aa.mobile is not null and aa.mobile != '' and aa.brand is not null
group by aa.mobile, aa.brand, aa.process_time

	



	





	


