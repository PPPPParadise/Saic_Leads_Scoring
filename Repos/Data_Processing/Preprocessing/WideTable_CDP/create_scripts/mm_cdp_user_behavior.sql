USE marketing_modeling;
DROP TABLE IF EXISTS mm_cdp_user_behavior;
CREATE TABLE IF NOT EXISTS mm_cdp_user_behavior (
    id                            	   bigint      comment    '用户唯一ID，phone num',
    is_app_user                        int         comment    '是否注册APP',
    brwose_count_d14                   int         comment    '过去14天浏览官网页面次数',
    avg_browse_time_d14                int         comment    '过去14天访问APP平均时长', 
    last_app_visit_time                int         comment    '最近一次访问APP时间', 
    app_point_ttl_d14                  int         comment    '过去14天取得积分总数',   
    app_point_ttl                      int         comment    '总使用积分次数', 
    last_app_point_use_time            int         comment    '最近一次使用积分时间',    

    is_trail_user                      int         comment    '是否试乘试驾', 
    trail_vel                          string      comment    '试驾车系', 
    last_trail_vel                     string      comment    '最近一次试驾车系',   
    deliver_vel                        string      comment    '购买车系',   
    visit_count                        int         comment    '总到店次数',      
    visit_count_d14                    int         comment    '过去14天到店次数',
    deal_time                          string      comment    '成交日期',
    last_trail_time                    int         comment    '最近一次试驾日期',
    last_lead_time                     int         comment    '最近跟进日期',
    trail_count_d14                    int         comment    '过去14天预约试乘试驾次数',
    last_reach_platform                string      comment    '最近触达平台',
    lead_sources                       string      comment     '线索来源',     
    last_bbs_time                      int         comment     '最近一次查看资讯论坛时间',
    order_time                         string      comment     'APP下订日期',  
    last_sis_result                    string      comment     '上次外呼结果',
    open_link                          int         comment     '是否打开短链',
    last_visit_time                    string      comment     '最近一次进店时间',  
    last_activity_time                 string      comment     '最近参加活动日期',
    create_card_count          		   int         comment     '跨店建卡数量',  
    cust_live_period                   int         comment     '客户寿命(当前时间-客户最早留资时间)',
    followup_ttl                       int         comment     '历史跟进次数',
    last_sis_time                      string      comment     '上次外呼日期',  
    deal_fail_time                     string      comment     '战败日期',  
    lead_model                         string      comment     '留资车系',
    fail_status                        int         comment     '战败核实状态',
    rx5_video_time                     int         comment     'RX5 MAX视频观看记录(APP端RX5 MAX视频观看类型)',
    marvel_video_time                  int         comment     'MARVELX视频观看记录',
    sex                                string      comment     '性别',
	age								   string	   comment	   '年龄',
	province                           string      comment     '省份',  
	city                               string      comment     '城市',  
    last_lost_type                     int         comment     '最近流失预警类型',
    last_lost_time                     int         comment     '最近流失预警日期',
    brwose_homepage              	   int   	   comment     '是否浏览爱车展厅了解详情车型及配置页',     
    search_vel                   	   int         comment     '是否搜索过车型关键词',

    browse_model_info            	   int         comment     '是否观看过车型的资讯',
    trail                        	   int         comment     '是否预约过车型试驾',
    make_deal                    	   int         comment     '是否下订',
    model                        	   int         comment     '关注车系',
    register_time                      int         comment     'APP注册时间',
    last_cust_base_time                int         comment     '最近一次建卡时间',
    fail_real_result                   int         comment     '战败核实结果',
    app_browse_count_d14               int         comment     '过去14天浏览APP次数'
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

