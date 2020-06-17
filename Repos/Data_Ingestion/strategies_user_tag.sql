SET mapreduce.map.memory.mb=4096;
SET mapreduce.reduce.memory.mb = 8192;

DROP TABLE IF EXISTS marketing_modeling.mm_stretegies_tags;
CREATE TABLE marketing_modeling.mm_stretegies_tags AS
SELECT  a.mobile                                                                   AS mobile 
       ,a.d_trail_book_tll                                                         AS trail_booking_ttl 
       ,a.c_last_trail_time                                                        AS last_trail_time 
       ,a.d_leads_car_model_count                                                  AS car_model_ttl 
       ,(case WHEN a.d_followup_d30 == 0 AND a.d_deal_flag == 0 THEN 1 else 0 end) AS no_deal 
       ,a.h_goal                                                                   AS buying_goal 
       ,a.h_loan                                                                   AS loan_ppt 
       ,a.h_priority_car_type                                                      AS car_type 
       ,a.h_priority_config_prefer                                                 AS config_prefer 
       ,a.h_priority_change_car_and_buy                                            AS change_and_buy_ppt 
       ,a.h_models                                                                 AS focus_models 
       ,a.h_budget                                                                 AS budget 
       ,a.h_model_nums                                                             AS model_nums 
       ,b.media                                                                    AS media_source 
       ,b.creative                                                                 AS media_content 
       ,c.read_article_ttl_d15 
       ,c.active_browse_time_ttl_d7 
       ,(case WHEN c.click_finance_service_d45 > 0 THEN 1 else 0 end)              AS click_finance_service
FROM marketing_modeling.mm_big_wide_info a
LEFT JOIN marketing_modeling.mm_dmp_user_info b
ON a.mobile = b.phone
LEFT JOIN marketing_modeling.mm_online_user_behavior c
ON a.mobile = c.phone