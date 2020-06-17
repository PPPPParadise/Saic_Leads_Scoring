USE marketing_modeling;
DROP TABLE IF EXISTS mm_online_user_behavior;
CREATE TABLE IF NOT EXISTS mm_online_user_behavior (
    cust_id                            bigint      comment    '用户唯一ID，phone num',
    click_finance_service_d7           int         comment    '过去7天点击金融服务次数',
    click_finance_service_d15          int         comment    '过去15天点击金融服务次数',     
    click_finance_service_d30          int         comment    '过去30天点击金融服务次数',
    click_finance_service_d45          int         comment    '过去45天点击金融服务次数', 
    click_MG_page_d7                   int         comment    '过去7天点击MG车系页总次数', 
    click_MG_page_d15                  int         comment    '过去15天点击MG车系页总次数',   
    click_MG_page_d30                  int         comment    '过去30天点击MG车系页总次数', 
    click_MG_page_d45                  int         comment    '过去45天点击MG车系页总次数',    

    click_MG_price_config_d7           int         comment    '过去7天点击名爵车系页-价格与配置页总次数', 
    click_MG_price_config_d15          int         comment    '过去15天点击名爵车系页-价格与配置页总次数', 
    click_MG_price_config_d30          int         comment    '过去30天点击名爵车系页-价格与配置页总次数',   
    click_MG_price_config_d45          int         comment    '过去45天点击名爵车系页-价格与配置页总次数',   
    click_MG_highlight_d7              int         comment    '过去7天点击名爵车系页-产品亮点页总次数',      
    click_MG_highlight_d15             int         comment    '过去15天点击名爵车系页-产品亮点页总次数',
    click_MG_highlight_d30             int         comment    '过去30天点击名爵车系页-产品亮点页总次数',
    click_MG_highlight_d45             int         comment    '过去45天点击名爵车系页-产品亮点页总次数'
    click_MG_360_d7                    int         comment    '过去7天点击名爵车系页-360赏析页总次数',
    click_MG_360_d15                   int         comment    '过去15天点击名爵车系页-360赏析页总次数',
    click_MG_360_d30                   int         comment    '过去30天点击名爵车系页-360赏析页总次数',
    click_MG_360_d45                   int         comment    '过去45天点击名爵车系页-360赏析页总次数',     
    click_MG_model_d7                  int         comment    '过去7天点击车系页涉及车型数',
    click_MG_model_d15                 int         comment    '过去15天点击车系页涉及车型数',  
    click_MG_model_d30                 int         comment    '过去30天点击车系页涉及车型数',
    click_MG_model_d45                 int         comment    '过去45天点击车系页涉及车型数',
    click_virtual_button_ttl           int         comment    '点击爱车-虚拟车控按钮',  
    click_slideshow_ttl                int         comment    '点击爱车-虚拟车控按钮',
    click_find_dealer_ttl              int         comment    '查找经销商',  

    click_buying_online_ttl            int         comment     '点击在线购车',
    active_browse_time_ttl_d7          int         comment     '7天内在App/小程序/官网浏览时间大于5分钟的次数',
    browse_mins_d7                     float       comment     '7天内在App/小程序/官网浏览总分钟数',  
    active_browse_time_ttl_d15         int         comment     '15天内在App/小程序/官网浏览时间大于5分钟的次数',  
    browse_mins_d15                    float       comment     '15天内在App/小程序/官网浏览总分钟数',  
    login_time_ttl_d30                 int         comment     '30天内客户在App/小程序/官网登录总次数',
    read_article_ttl_d7                int         comment     '7天内客户在App/小程序阅读文章总数量',
    read_article_ttl_d15               int         comment     '15天内客户在App/小程序阅读文章总数量',
    avg_browse_ttl_d45                 int         comment     '把过去45天以15天为一个窗口切成三个15天，计算每个15天内的线上渠道 (官网/App) 总浏览时长，计算三个时间窗口浏览时长的平均值',
    avg_browse_dif_d45                 int         comment     '把过去45天以15天为一个窗口切成三个15天，计算每个15天内的线上渠道 (官网/App) 总浏览时长，1-15天的总浏览时长减去31-45天的总浏览时长得到的差值',  
   )
ROW FORMAT SERDE 
	  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
WITH SERDEPROPERTIES ( 
	  'field.delim'='\t', 
	  'serialization.format'='\t') 
STORED AS INPUTFORMAT 
	  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
	  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'



