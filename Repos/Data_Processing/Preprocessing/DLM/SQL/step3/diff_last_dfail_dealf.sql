set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
set hive.mapjoin.smalltable.filesize=55000000;
set hive.auto.convert.join=false;  -- #取消小表加载至内存中

DROP TABLE IF EXISTS marketing_modeling.dlm_tmp_last_dfail_dealf_diff;

CREATE TABLE marketing_modeling.dlm_tmp_last_dfail_dealf_diff as 
SELECT 
	coalesce(a1.mobile,a2.mobile) as mobile,
	if(a1.last_dfail_dealf_diff is null,a2.last_dfail_dealf_diff,a1.last_dfail_dealf_diff) as last_dfail_dealf_diff
FROM
(	
	SELECT
		a.mobile, 
		datediff(a.last_time,b.last_time ) as last_dfail_dealf_diff	  ---客户最后一次战败日期距成交日期天数
	FROM
		marketing_modeling.dlm_tmp_deliver_processing a,
		marketing_modeling.dlm_tmp_fail_processing b
	WHERE
		a.mobile = b.mobile and b.last_time > a.last_time
) a1
full join 
(	
	SELECT
		a.mobile, 
		datediff(a.last_time,a.second_time ) as last_dfail_dealf_diff	  ---客户最后一次战败日期距(倒数第二次战败)日期天数
	FROM
		marketing_modeling.dlm_tmp_fail_processing a
) a2 on a1.mobile = a2.mobile
;