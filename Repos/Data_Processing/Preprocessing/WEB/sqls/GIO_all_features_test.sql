

    from marketing_modeling.gio_tmp_360view_tb
    from marketing_modeling.gio_tmp_finance_tb
    from marketing_modeling.gio_tmp_mg_model_tb
    from marketing_modeling.gio_tmp_price_config_tb
    from marketing_modeling.gio_tmp_product_spotlight_tb
    from marketing_modeling.gio_tmp_model_page_tb
LEFT JOIN marketing_modeling.gio_tmp_virtual_tb z on a.device_id = z.device_id
LEFT JOIN marketing_modeling.gio_tmp_slideshow_tb aa on a.device_id = aa.device_id
LEFT JOIN marketing_modeling.gio_tmp_find_dealers_tb ab on a.device_id = ab.device_id
LEFT JOIN marketing_modeling.gio_tmp_buy_online_tb ac on a.device_id = ac.device_id
    from marketing_modeling.gio_tmp_app_web_browsing_tb
LEFT JOIN marketing_modeling.gio_tmp_ttl_browse_time_d7_tb 
    from marketing_modeling.gio_tmp_app_web_browsing_tb
LEFT JOIN marketing_modeling.gio_tmp_ttl_browse_time_d15_tb 
LEFT JOIN marketing_modeling.gio_tmp_login_tb 
LEFT JOIN marketing_modeling.gio_tmp_ttl_browse_time_avg45_tb
LEFT JOIN marketing_modeling.gio_tmp_ttl_browse_time_diff_tb
    from marketing_modeling.gio_tmp_article_tb
    from marketing_modeling.gio_tmp_article_tb


select device_id,count(*) c from marketing_modeling.gio_tmp_360view_tb group by device_id order by c desc limit 10;
select device_id,count(*) c from marketing_modeling.gio_tmp_finance_tb group by device_id order by c desc limit 10;
select device_id,count(*) c from marketing_modeling.gio_tmp_mg_model_tb group by device_id order by c desc limit 10;
select device_id,count(*) c from marketing_modeling.gio_tmp_price_config_tb group by device_id order by c desc limit 10;
select device_id,count(*) c from marketing_modeling.gio_tmp_product_spotlight_tb group by device_id order by c desc limit 10;
select device_id,count(*) c from marketing_modeling.gio_tmp_model_page_tb group by device_id order by c desc limit 10;
select device_id,count(*) c from marketing_modeling.gio_tmp_virtual_tb group by device_id order by c desc limit 10;
select device_id,count(*) c from marketing_modeling.gio_tmp_slideshow_tb group by device_id order by c desc limit 10;
select device_id,count(*) c from marketing_modeling.gio_tmp_find_dealers_tb group by device_id order by c desc limit 10;
select device_id,count(*) c from marketing_modeling.gio_tmp_buy_online_tb group by device_id order by c desc limit 10;
select device_id,count(*) c from marketing_modeling.gio_tmp_app_web_browsing_tb group by device_id order by c desc limit 10;
select device_id,count(*) c from marketing_modeling.gio_tmp_ttl_browse_time_d7_tb  group by device_id order by c desc limit 10;
select device_id,count(*) c from marketing_modeling.gio_tmp_ttl_browse_time_d15_tb group by device_id order by c desc limit 10;
select device_id,count(*) c from marketing_modeling.gio_tmp_login_tb group by device_id order by c desc limit 10;
select device_id,count(*) c from marketing_modeling.gio_tmp_ttl_browse_time_avg45_tb group by device_id order by c desc limit 10;
select device_id,count(*) c from marketing_modeling.gio_tmp_ttl_browse_time_diff_tb group by device_id order by c desc limit 10;
select device_id,count(*) c from marketing_modeling.gio_tmp_article_tb group by device_id order by c desc limit 10;
