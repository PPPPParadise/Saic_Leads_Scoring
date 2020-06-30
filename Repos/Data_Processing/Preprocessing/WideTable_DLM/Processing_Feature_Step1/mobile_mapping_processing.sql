---- 
set tez.queue.name=${queuename};

DROP TABLE IF EXISTS marketing_modeling.tmp_dlm_mobile_mapping;
CREATE TABLE marketing_modeling.tmp_dlm_mobile_mapping as
select mobile from marketing_modeling.tmp_dlm_leads_cleansing group by mobile
union
select mobile from marketing_modeling.tmp_dlm_cust_cleansing group by mobile 
union
select mobile from marketing_modeling.tmp_dlm_hall_flow_cleansing group by mobile
;