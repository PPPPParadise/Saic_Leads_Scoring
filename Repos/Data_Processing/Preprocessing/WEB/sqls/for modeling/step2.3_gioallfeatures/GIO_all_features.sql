set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;

drop table if exists marketing_modeling.mm_online_user_behavior;
create table marketing_modeling.mm_online_user_behavior as 
SELECT 
	a.mobile as phone,
    a.device_id as device_id,
	a.deal_flag as deal_flag,                   --成交/战败状态
	a.status_date as status_date,		  --成交/战败时间
    f.finance  as click_finance_service_d7, --过去7天点击金融服务次数
    g.finance  as click_finance_service_d15,--过去15天点击金融服务次数
    h.finance  as click_finance_service_d30,--过去30天点击金融服务次数
    i.finance  as click_finance_service_d45,--过去45天点击金融服务次数
    j.model  as  click_MG_page_d7,			--过去7天点击MG车系页总次数
    k.model  as click_MG_page_d15,			--过去15天点击MG车系页总次数
    l.model  as click_MG_page_d30,			--过去30天点击MG车系页总次数
    m.model  as click_MG_page_d45,			--过去45天点击MG车系页总次数
    n.price_config  as click_MG_price_config_d7,  --过去7天点击名爵车系页-价格与配置页总次数
    o.price_config  as click_MG_price_config_d15, --过去15天点击名爵车系页-价格与配置页总次数
    p.price_config  as click_MG_price_config_d30, --过去30天点击名爵车系页-价格与配置页总次数
    q.price_config  as click_MG_price_config_d45, --过去45天点击名爵车系页-价格与配置页总次数
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
    (marketing_modeling.deal_flag_date_map a
LEFT JOIN
    marketing_modeling.gio_tmp_360view_tb b
ON a.device_id = b.device_id and b.type = 7
LEFT JOIN
      marketing_modeling.gio_tmp_360view_tb c
ON a.device_id = c.device_id and c.type = 15
LEFT JOIN
	  marketing_modeling.gio_tmp_360view_tb d
ON a.device_id = d.device_id and d.type = 30
LEFT JOIN
	  marketing_modeling.gio_tmp_360view_tb e
ON a.device_id = e.device_id and e.type = 45
LEFT JOIN
	  marketing_modeling.gio_tmp_finance_tb f
ON a.device_id = f.device_id and f.type = 7
LEFT JOIN
	  marketing_modeling.gio_tmp_finance_tb g
ON a.device_id = g.device_id and g.type = 15
LEFT JOIN
	  marketing_modeling.gio_tmp_finance_tb h
ON a.device_id = h.device_id and h.type = 30
LEFT JOIN
	  marketing_modeling.gio_tmp_finance_tb i
ON a.device_id = i.device_id and i.type = 45
LEFT JOIN
	marketing_modeling.gio_tmp_mg_model_tb j
ON a.device_id = j.device_id and j.type = 7
LEFT JOIN
marketing_modeling.gio_tmp_mg_model_tb k
ON a.device_id = k.device_id and k.type = 15
LEFT JOIN
marketing_modeling.gio_tmp_mg_model_tb l
ON l.device_id = l.device_id and l.type = 30
LEFT JOIN
marketing_modeling.gio_tmp_mg_model_tb m
ON a.device_id = m.device_id and m.type = 45
LEFT JOIN
	marketing_modeling.gio_tmp_price_config_tb n
ON a.device_id = n.device_id and n.type = 7
LEFT JOIN
	marketing_modeling.gio_tmp_price_config_tb o
 ON a.device_id = o.device_id and o.type = 15
LEFT JOIN
	marketing_modeling.gio_tmp_price_config_tb p
 ON a.device_id = p.device_id and p.type = 30
LEFT JOIN
	marketing_modeling.gio_tmp_price_config_tb q
 ON a.device_id = q.device_id and q.type = 45
LEFT JOIN
	marketing_modeling.gio_tmp_product_spotlight_tb r
 ON a.device_id = r.device_id and r.type = 7
LEFT JOIN
	marketing_modeling.gio_tmp_product_spotlight_tb s
 ON a.device_id = s.device_id and s.type = 15
LEFT JOIN
	marketing_modeling.gio_tmp_product_spotlight_tb t
 ON a.device_id = t.device_id and t.type = 30
LEFT JOIN
marketing_modeling.gio_tmp_product_spotlight_tb u
 ON a.device_id = u.device_id and u.type = 45
LEFT JOIN
	marketing_modeling.gio_tmp_model_page_tb v
ON a.device_id = v.device_id and v.type = 7
LEFT JOIN
(select *
	marketing_modeling.gio_tmp_model_page_tb w
ON a.device_id = w.device_id and w.type = 15
LEFT JOIN
	marketing_modeling.gio_tmp_model_page_tb x
ON a.device_id = x.device_id and x.type = 30
LEFT JOIN
	marketing_modeling.gio_tmp_model_page_tb y
ON a.device_id = y.device_id and y.type = 45

LEFT JOIN marketing_modeling.gio_tmp_virtual_tb z on a.device_id = z.device_id
LEFT JOIN marketing_modeling.gio_tmp_slideshow_tb aa on a.device_id = aa.device_id
LEFT JOIN marketing_modeling.gio_tmp_find_dealers_tb ab on a.device_id = ab.device_id
LEFT JOIN marketing_modeling.gio_tmp_buy_online_tb ac on a.device_id = ac.device_id
LEFT JOIN 
	marketing_modeling.gio_tmp_app_web_browsing_tb ad
ON a.device_id = ad.device_id and ad.period = 7
LEFT JOIN marketing_modeling.gio_tmp_ttl_browse_time_d7_tb ae on a.device_id = ae.device_id
LEFT JOIN
	marketing_modeling.gio_tmp_app_web_browsing_tb af
ON a.device_id = af.device_id and af.period = 15
LEFT JOIN marketing_modeling.gio_tmp_ttl_browse_time_d15_tb ag on a.device_id = ag.device_id
LEFT JOIN marketing_modeling.gio_tmp_login_tb ah on a.device_id = ah.device_id
LEFT JOIN marketing_modeling.gio_tmp_ttl_browse_time_avg45_tb ai on a.device_id = ai.device_id
LEFT JOIN marketing_modeling.gio_tmp_ttl_browse_time_diff_tb aj on a.device_id = aj.device_id
LEFT JOIN
	marketing_modeling.gio_tmp_article_tb ak
ON a.device_id = ak.device_id and ak.type = 7
LEFT JOIN
	marketing_modeling.gio_tmp_article_tb al
ON a.device_id = al.device_id and al.type = 15
);