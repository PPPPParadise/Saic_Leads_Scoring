set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
set hive.mapjoin.smalltable.filesize=55000000;
set hive.auto.convert.join=false;  -- #取消小表加载至内存中
set mapreduce.job.queuename=${queuename};
---- 意向信息清洗

DROP TABLE IF EXISTS marketing_modeling.tmp_dlm_oppor_cleansing;
CREATE TABLE marketing_modeling.tmp_dlm_oppor_cleansing as
SELECT 
	a.cust_id,
	b.mobile,
	a.dealer_id,
	a.brand_id,
	a.vel_series_id,
	a.vel_model_id,
	a.deposit_order_create_time,
	a.is_deposit_order,
	a.series_brand_id,
	a.series_type,
	a.series_code,
	a.create_time
FROM 
	(
		SELECT 
			o.cust_id,
			o.dealer_id,
			o.brand_id,
			o.vel_series_id,
			o.vel_model_id,
			o.deposit_order_create_time,   -- 下定时间
			if(o.deposit_order_create_time is null,0,1) as is_deposit_order,  --是否下定 
			o.create_time,
			c.brand_id as series_brand_id,
			case 
				when c.series_code = 'MG3' then 'CAR' 
				when c.series_code = 'MG750' then 'CAR' 
				when c.series_code = 'MG350' then 'CAR' 
				when c.series_code = 'MGGT' then 'CAR' 
				when c.series_code = 'MG360' then 'CAR' 
				when c.series_code = 'MGZS' then 'SUV' 
				when c.series_code = 'MGRX5' then 'SUV' 
				when c.series_code = 'IP32P' then 'CAR' 
				when c.series_code = 'AS23' then 'SUV' 
				when c.series_code = 'AP31M' then 'CAR' 
				when c.series_code = 'MGRX8' then 'SUV' 
				when c.series_code = 'MG550' then 'CAR' 
				when c.series_code = 'MG7' then 'CAR' 
				when c.series_code = 'MGTF' then 'TFCAR' 
				when c.series_code = 'MG6' then 'CAR' 
				when c.series_code = 'MG5' then 'CAR' 
				when c.series_code = 'MGGS' then 'SUV' 
				when c.series_code = 'ZS11E' then 'SUV' 
				when c.series_code = 'AS23P' then 'SUV' 
				when c.series_code = 'EP22X1' then 'CAR' 
				else 'RW_CAR'
			end as series_type,
			c.series_code
		FROM
			dtwarehouse.ods_dlm_t_oppor o,
			dtwarehouse.cdm_dim_series c
		WHERE o.vel_series_id = c.series_dol_product_id
		  AND o.create_time >= '${beginTime}' 
		  AND o.cust_id is not null 
		  and o.pt = '${pt}'
	) a,
	marketing_modeling.tmp_dlm_cust_cleansing b 
where a.cust_id = b.cust_id and a.dealer_id = b.dealer_id 
	;
  
