--*姓名，电话，地址，公司名称，经销商代码，城市，省份

DROP TABLE IF EXISTS marketing_modeling.ob_cust_dealer_info;
CREATE TABLE marketing_modeling.ob_cust_dealer_info as
SELECT 
    a.mobile as mobile,
    a.pred_score as fail_leads_oppor_score,
    a.d_last_dealfail_d as fail_time,
    a.c_province as province,
    a.c_city as city,
    a.c_sex as sex,
    a.c_age as age,
    a.second_resource_name as fail_leads_source,
    b.id as cust_id,
    b.name as name,
    b.dealer_id as dealer_id,
    c.dealer_code as dealer_code,
    c.parent_dealer_code as parent_dealer_code,
    c.parent_dealer_shortname as parent_dealer_shortname,
    c.address as address,
    c.city_name as city_name,
    c.area as area,
    e.series_chinese_name as oppor_series,
    f.fail_reason_fir_level as fail_reason,
    MAX(b.create_time) OVER(PARTITION BY b.mobile) as last_time,
    ROW_NUMBER() OVER(PARTITION BY b.mobile ORDER BY b.create_time DESC) AS rn
FROM 
    (select * from marketing_modeling.outbound_model_result where pt = '${pt}' and score_rank <= '${top}')a
LEFT JOIN
    (
        SELECT * FROM dtwarehouse.ods_dlm_t_cust_base           -- 建卡表
        WHERE 
          mobile regexp "^[1][3-9][0-9]{9}$"  
          AND pt = '${pt}'
    ) b
on a.mobile = b.mobile
LEFT JOIN
    (SELECT * FROM dtwarehouse.ods_rdp_v_sales_region_dealer WHERE pt = '${pt}') c 
on b.dealer_id = c.dlm_org_id
LEFT JOIN
    (SELECT * from dtwarehouse.ods_dlm_t_oppor WHERE pt = '${pt}')d
on b.id = d.cust_id
LEFT JOIN
    dtwarehouse.cdm_dim_series e
on d.vel_series_id = e.series_dol_product_id
LEFT JOIN
    marketing_modeling.app_failed_reason f 
on a.mobile = f.mobile
WHERE 
    c.brand_id = 121;