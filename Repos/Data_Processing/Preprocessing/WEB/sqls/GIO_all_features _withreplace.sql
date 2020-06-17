set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;

drop table if exists marketing_modeling.gio_tmp_allfeatures_tb;
create table marketing_modeling.gio_tmp_allfeatures_tb as 
SELECT 

    a.device_id as device_id,
    CASE WHEN f.finance IS NULL THEN 0 ELSE f.finance END as click_finance_service_d7,
    CASE WHEN g.finance IS NULL THEN 0 ELSE g.finance END as click_finance_service_d15,
    CASE WHEN h.finance IS NULL THEN 0 ELSE h.finance END as click_finance_service_d30,
    CASE WHEN i.finance IS NULL THEN 0 ELSE i.finance END as click_finance_service_d45,
    CASE WHEN j.model IS NULL THEN 0 ELSE j.model END as  click_MG_page_d7,
    CASE WHEN k.model IS NULL THEN 0 ELSE k.model END as click_MG_page_d15,
    CASE WHEN l.model IS NULL THEN 0 ELSE l.model END as click_MG_page_d30,
    CASE WHEN m.model IS NULL THEN 0 ELSE m.model END as click_MG_page_d45,
    CASE WHEN n.price_config IS NULL THEN 0 ELSE n.price_config END as click_MG_price_config_d7,
    CASE WHEN o.price_config IS NULL THEN 0 ELSE o.price_config END as click_MG_price_config_d15,
    CASE WHEN p.price_config IS NULL THEN 0 ELSE p.price_config END as click_MG_price_config_d30,
    CASE WHEN q.price_config IS NULL THEN 0 ELSE q.price_config END as click_MG_price_config_d45,
    CASE WHEN r.prdt_spotlight IS NULL THEN 0 ELSE r.prdt_spotlight END as click_MG_highlight_d7,
    CASE WHEN s.prdt_spotlight IS NULL THEN 0 ELSE s.prdt_spotlight END as click_MG_highlight_d15,
    CASE WHEN t.prdt_spotlight IS NULL THEN 0 ELSE t.prdt_spotlight END as click_MG_highlight_d30,
    CASE WHEN u.prdt_spotlight IS NULL THEN 0 ELSE u.prdt_spotlight END as click_MG_highlight_d45,
    CASE WHEN b.view_360 IS NULL THEN 0 ELSE b.view_360 END as click_MG_360_d7,
    CASE WHEN c.view_360 IS NULL THEN 0 ELSE c.view_360 END as click_MG_360_d15,
    CASE WHEN d.view_360 IS NULL THEN 0 ELSE d.view_360 END as click_MG_360_d30,
    CASE WHEN e.view_360 IS NULL THEN 0 ELSE e.view_360 END as click_MG_360_d45,
    CASE WHEN v.model_page IS NULL THEN 0 ELSE v.model_page END as click_MG_model_d7,
    CASE WHEN w.model_page IS NULL THEN 0 ELSE w.model_page END as click_MG_model_d15,
    CASE WHEN x.model_page IS NULL THEN 0 ELSE x.model_page END as click_MG_model_d30,
    CASE WHEN y.model_page IS NULL THEN 0 ELSE y.model_page END as click_MG_model_d45,
    CASE WHEN z.lovecar_virtual_botton IS NULL THEN 0 ELSE z.lovecar_virtual_botton END as click_virtual_button_ttl,
    CASE WHEN aa.lovecar_slideshow IS NULL THEN 0 ELSE aa.lovecar_slideshow END as click_slideshow_ttl,
    CASE WHEN ab.result IS NULL THEN 0 ELSE ab.result END as click_find_dealer_ttl,
    CASE WHEN ac.buying_online IS NULL THEN 0 ELSE ac.buying_online END as click_buying_online_ttl,
    CASE WHEN ad.nums IS NULL THEN 0 ELSE ad.nums END as active_browse_time_ttl_d7,
    CASE WHEN ae.dur_7 IS NULL THEN 0 ELSE ae.dur_7 END as browse_mins_d7,
    CASE WHEN af.nums IS NULL THEN 0 ELSE af.nums END as active_browse_time_ttl_d15,
    CASE WHEN ag.dur_15 IS NULL THEN 0 ELSE ag.dur_15 END as browse_mins_d15,
    CASE WHEN ah.login IS NULL THEN 0 ELSE ah.login END as login_time_ttl_d30,
    CASE WHEN ai.dur_avg IS NULL THEN 0 ELSE ai.dur_avg END as avg_browse_ttl_d45,
    CASE WHEN aj.dur_diff IS NULL THEN 0 ELSE aj.dur_diff END as avg_browse_dif_d45,
    CASE WHEN ak.ttl IS NULL THEN 0 ELSE ak.ttl END as read_article_ttl_d7,
    CASE WHEN al.ttl IS NULL THEN 0 ELSE al.ttl END as read_article_ttl_d15
FROM 
    ((select phone,device_id 
	  from 
	  rdtwarehouse.cdm_growingio_mapping_hma
	  where phone is not null and 
			device_id is not null
	  group by phone,device_id ) a1
LEFT JOIN																
	(select DISTINCT(device_id)
    from
    rdtwarehouse.cdm_growingio_action_hma
    where device_id is not null and device_id <> '')a
ON a1.device_id = a.device_id
LEFT JOIN
    (select *
    from marketing_modeling.gio_tmp_360view_tb
    where type = 7)b
ON a.device_id = b.device_id
LEFT JOIN
    (select *
    from marketing_modeling.gio_tmp_360view_tb
    where type = 15)c
ON a.device_id = c.device_id
LEFT JOIN
    (select *
    from marketing_modeling.gio_tmp_360view_tb
    where type = 30)d
ON a.device_id = d.device_id
LEFT JOIN
    (select *
    from marketing_modeling.gio_tmp_360view_tb
    where type = 45)e
ON a.device_id = e.device_id
LEFT JOIN
(select *
    from marketing_modeling.gio_tmp_finance_tb
    where type = 7)f
ON a.device_id = f.device_id
LEFT JOIN
(select *
    from marketing_modeling.gio_tmp_finance_tb
    where type = 15)g
ON a.device_id = g.device_id
LEFT JOIN
(select *
    from marketing_modeling.gio_tmp_finance_tb
    where type = 30)h
ON a.device_id = h.device_id
LEFT JOIN
(select *
    from marketing_modeling.gio_tmp_finance_tb
    where type = 45)i
ON a.device_id = i.device_id
LEFT JOIN
(select *
    from marketing_modeling.gio_tmp_mg_model_tb
    where type = 7)j
ON a.device_id = j.device_id
LEFT JOIN
(select *
    from marketing_modeling.gio_tmp_mg_model_tb
    where type = 15)k
ON a.device_id = k.device_id
LEFT JOIN
(select *
    from marketing_modeling.gio_tmp_mg_model_tb
    where type = 30)l
ON a.device_id = l.device_id
LEFT JOIN
(select *
    from marketing_modeling.gio_tmp_mg_model_tb
    where type = 45)m
ON a.device_id = m.device_id
LEFT JOIN
(select *
    from marketing_modeling.gio_tmp_price_config_tb
    where type = 7)n
ON a.device_id = n.device_id
LEFT JOIN
(select *
    from marketing_modeling.gio_tmp_price_config_tb
    where type = 15)o
ON a.device_id = o.device_id
LEFT JOIN
(select *
    from marketing_modeling.gio_tmp_price_config_tb
    where type = 30)p
ON a.device_id = p.device_id
LEFT JOIN
(select *
    from marketing_modeling.gio_tmp_price_config_tb
    where type = 45)q
ON a.device_id = q.device_id
LEFT JOIN
(select *
    from marketing_modeling.gio_tmp_product_spotlight_tb
    where type = 7)r
ON a.device_id = r.device_id
LEFT JOIN
(select *
    from marketing_modeling.gio_tmp_product_spotlight_tb
    where type = 15)s
ON a.device_id = s.device_id
LEFT JOIN
(select *
    from marketing_modeling.gio_tmp_product_spotlight_tb
    where type = 30)t
ON a.device_id = t.device_id
LEFT JOIN
(select *
    from marketing_modeling.gio_tmp_product_spotlight_tb
    where type = 45)u
ON a.device_id = u.device_id
LEFT JOIN
(select *
    from marketing_modeling.gio_tmp_model_page_tb
    where type = 7)v
ON a.device_id = v.device_id
LEFT JOIN
(select *
    from marketing_modeling.gio_tmp_model_page_tb
    where type = 15)w
ON a.device_id = w.device_id
LEFT JOIN
(select *
    from marketing_modeling.gio_tmp_model_page_tb
    where type = 30)x
ON a.device_id = x.device_id
LEFT JOIN
(select *
    from marketing_modeling.gio_tmp_model_page_tb
    where type = 45)y
ON a.device_id = y.device_id
LEFT JOIN marketing_modeling.gio_tmp_virtual_tb z on a.device_id = z.device_id
LEFT JOIN marketing_modeling.gio_tmp_slideshow_tb aa on a.device_id = aa.device_id
LEFT JOIN marketing_modeling.gio_tmp_find_dealers_tb ab on a.device_id = ab.device_id
LEFT JOIN marketing_modeling.gio_tmp_buy_online_tb ac on a.device_id = ac.device_id
LEFT JOIN 
(select *
    from marketing_modeling.gio_tmp_app_web_browsing_tb
    where period = 7)ad
ON a.device_id = ad.device_id
LEFT JOIN marketing_modeling.gio_tmp_ttl_browse_time_d7_tb ae on a.device_id = ae.device_id
LEFT JOIN
(select *
    from marketing_modeling.gio_tmp_app_web_browsing_tb
    where period = 15)af
ON a.device_id = af.device_id
LEFT JOIN marketing_modeling.gio_tmp_ttl_browse_time_d15_tb ag on a.device_id = ag.device_id
LEFT JOIN marketing_modeling.gio_tmp_login_tb ah on a.device_id = ah.device_id
LEFT JOIN marketing_modeling.gio_tmp_ttl_browse_time_avg45_tb ai on a.device_id = ai.device_id
LEFT JOIN marketing_modeling.gio_tmp_ttl_browse_time_diff_tb aj on a.device_id = aj.device_id
LEFT JOIN
(select *
    from marketing_modeling.gio_tmp_article_tb
    where type = 7)ak
ON a.device_id = ak.device_id
LEFT JOIN
(select *
    from marketing_modeling.gio_tmp_article_tb
    where type = 15)al
ON a.device_id = al.device_id
);