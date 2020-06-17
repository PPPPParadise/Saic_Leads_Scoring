set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;

drop table if exists marketing_modeling.gio_tmp_allfeatures_tb;
create table marketing_modeling.gio_tmp_allfeatures_tb as 
SELECT 
	a1.phone as phone,
    a1.device_id as device_id,
    f.finance  as click_finance_service_d7,
    g.finance  as click_finance_service_d15,
    h.finance  as click_finance_service_d30,
    i.finance  as click_finance_service_d45,
    j.model  as  click_MG_page_d7,
    k.model  as click_MG_page_d15,
    l.model  as click_MG_page_d30,
    m.model  as click_MG_page_d45,
    n.price_config  as click_MG_price_config_d7,
    o.price_config  as click_MG_price_config_d15,
    p.price_config  as click_MG_price_config_d30,
    q.price_config  as click_MG_price_config_d45,
    r.prdt_spotlight  as click_MG_highlight_d7,
    s.prdt_spotlight  as click_MG_highlight_d15,
    t.prdt_spotlight  as click_MG_highlight_d30,
    u.prdt_spotlight  as click_MG_highlight_d45,
    b.view_360  as click_MG_360_d7,
    c.view_360  as click_MG_360_d15,
    d.view_360  as click_MG_360_d30,
    e.view_360  as click_MG_360_d45,
    v.model_page  as click_MG_model_d7,
    w.model_page  as click_MG_model_d15,
    x.model_page  as click_MG_model_d30,
    y.model_page  as click_MG_model_d45,
    z.lovecar_virtual_botton  as click_virtual_button_ttl,
    aa.lovecar_slideshow  as click_slideshow_ttl,
    ab.result  as click_find_dealer_ttl,
    ac.buying_online  as click_buying_online_ttl,
    ad.nums  as active_browse_time_ttl_d7,
    ae.dur_7 as browse_mins_d7,
    af.nums as active_browse_time_ttl_d15,
    ag.dur_15 as browse_mins_d15,
    ah.login as login_time_ttl_d30,
    ai.dur_avg as avg_browse_ttl_d45,
    aj.dur_diff as avg_browse_dif_d45,
    ak.ttl as read_article_ttl_d7,
    al.ttl as read_article_ttl_d15
FROM 
    ((select phone,device_id 
	  from 
	  rdtwarehouse.cdm_growingio_mapping_hma 
	  where phone is not null and
			phone != '' and
			device_id is not null and
			device_id != ''
	  group by phone,device_id ) a1
LEFT JOIN																
	(select DISTINCT(device_id)
    from
    marketing_modeling.gio_tmp_deviceid
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