---- 建卡信息清洗

DROP TABLE IF EXISTS marketing_modeling.dlm_tmp_mobile_mapping;
CREATE TABLE marketing_modeling.dlm_tmp_mobile_mapping as
select mobile from marketing_modeling.dlm_tmp_leads_cleansing group by mobile
union
select mobile from marketing_modeling.dlm_tmp_cust_cleansing group by mobile 
union
select mobile from marketing_modeling.dlm_tmp_hall_flow_cleansing group by mobile
  
