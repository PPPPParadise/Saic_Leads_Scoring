DROP TABLE IF EXISTS marketing_modeling.app_big_wide_info_new;
CREATE TABLE IF NOT EXISTS marketing_modeling.app_big_wide_info_new (
	mobile string COMMENT '用户唯一ID，mobile',         
   d_fir_leads_time timestamp COMMENT '首次留资时间',   
   d_last_leads_time timestamp COMMENT '最后留资时间',  
   d_fir_sec_leads_diff int COMMENT '首次留资时间与二次留资时间日期差值',  
   d_leads_count bigint COMMENT '总留资次数',          
   d_leads_channel array<int> COMMENT '留资渠道',     
   d_avg_leads_date double COMMENT '平均留资时间间隔',    
   d_leads_channel_count bigint COMMENT '留资渠道总数',  
   d_leads_dtbt_count bigint COMMENT '留资总经销商数',   
   d_leads_dtbt_coincide double COMMENT '留资经销商重合度',  
   d_leads_dtbt_level_1 bigint COMMENT '一级经销商留资数量',  
   d_leads_dtbt_level_2 bigint COMMENT '二级经销商留资数量',  
   d_cust_type int COMMENT '客户属性 (个人或企业)',        
   d_fir_card_time timestamp COMMENT '首次建卡时间',    
   d_last_card_time timestamp COMMENT '首次建卡时间',   
   d_card_ttl bigint COMMENT '总建卡次数',             
   d_cust_ids array<bigint> COMMENT '建卡ID数组',     
   d_clue_issued_times bigint COMMENT '线索总下发次数',  
   d_fir_visit_time timestamp COMMENT '首次到店时间',   
   d_fir_sec_visit_diff int COMMENT '首次到店时间与二次到店时间日期差值',  
   d_visit_dtbt_count bigint COMMENT '到店总经销商数量',  
   d_visit_ttl bigint COMMENT '总到店次数',            
   d_avg_visit_date double COMMENT '平均到店时间间隔',    
   d_dealer_ids array<bigint> COMMENT ' 经销商ID数组',  
   d_avg_visit_dtbt_count double COMMENT '平均每个经销商处到店次数',  
   d_followup_ttl bigint COMMENT '总跟进次数',         
   d_fir_trail timestamp COMMENT '-首次试乘试驾时间',     
   d_last_reservation_time timestamp COMMENT ' 最后一次试驾预约试驾',  
   d_trail_book_tll bigint COMMENT '-试乘试驾总预定次数',  
   d_trail_attend_ttl bigint COMMENT '-试乘试驾总参与次数',  
   d_trail_attend_ppt double COMMENT '-试乘试驾参与率',  
   d_leads_car_model_count bigint COMMENT '留资车型数量',  
   d_leads_car_model_type bigint COMMENT '留资车型种类数',  
   d_vel_series_ids array<bigint> COMMENT '车型IDs',  
   d_series_types array<string> COMMENT '车型种类IDs',  
   d_rw_car_model_count bigint COMMENT '荣威留资车型数量',  
   d_deal_time timestamp COMMENT ' 首次成交时间',       
   d_last_deal_time timestamp COMMENT ' 最后成交时间',  
   d_deal_ttl bigint COMMENT ' 总成交次数',            
   d_deal_car_ttl bigint COMMENT ' 两年内总购买车辆数',    
   d_fir_dealfail_d timestamp COMMENT '- 首次战败时间',  
   d_last_dealfail_d timestamp COMMENT ' 最后战败时间',  
   d_is_deposit_order int COMMENT '-是否下定',        
   d_deal_flag int COMMENT '- 是否成交 1-成交，0-战败，null-跟进',  
   d_leads_pool_status string COMMENT '状态：001-未下发；002-培育；003-跟进',  
   d_dealf_succ_firvisit_diff double COMMENT '成交时间与成交经销商处首次到店时间日期差值',  
   d_dealf_succ_lastvisit_diff double COMMENT '成交时间与成交经销商处最后一次到店时间日期差值',  
   d_avg_fir_sec_visit_diff double COMMENT '平均首次到店时间与二次到店时间日期差值',  
   d_fircard_firvisit_dtbt_diff array<int> COMMENT '建卡时间距首次到店时间日期差值(每个经销商有个value)',  
   d_avg_fircard_firvisit_diff double COMMENT '平均建卡时间距首次到店时间日期差值',  
   d_avg_firleads_firvisit_diff double COMMENT '平均首次留资时间距首次到店时间日期差值',  
   d_fir_activity_time timestamp COMMENT '首次活动时间',  
   d_activity_ttl bigint COMMENT '总活动次数',         
   d_natural_visit string COMMENT '自然到店',         
   d_fir_leads_deal_diff_y double COMMENT '上次成交时间距首次留资时间年数',  
   d_has_deliver_history int COMMENT '之前是否荣威/名爵车主',  
   d_trail_count_d30 bigint COMMENT '去30天总试驾数',  
   d_trail_count_d90 bigint COMMENT '过去30天总试驾数',  
   d_visit_count_d15 bigint COMMENT '过去15天总到店次数',  
   d_visit_count_d30 bigint COMMENT '过去30天总到店次数',  
   d_visit_count_d90 bigint COMMENT '过去90天总到店次数',  
   d_followup_d7 bigint COMMENT '过去7天跟进次数',       
   d_followup_d15 bigint COMMENT '过去15天跟进次数',     
   d_followup_d30 bigint COMMENT '过去30天跟进次数',     
   d_followup_d60 bigint COMMENT '过去60天跟进次数',     
   d_followup_d90 bigint COMMENT '过去90天跟进次数',     
   d_activity_count_d15 bigint COMMENT '过去15天参加活动次数',  
   d_activity_count_d30 bigint COMMENT '过去30天参加活动次数',  
   d_activity_count_d60 bigint COMMENT '过去60天参加活动次数',  
   d_activity_count_d90 bigint COMMENT '过去90天参加活动次数',  
   d_firlead_dealf_diff int COMMENT '首次留资时间距最后成交/战败时间差值',  
   d_lastlead_dealf_diff int COMMENT '最后一次留资时间距最后成交/战败时间差值',  
   d_last_dfail_dealf_diff int COMMENT '客户最后一次战败日期距成交/战败日期天数',  
   d_firfollow_dealf_diff int COMMENT '首次跟进日期距成交/战败时间差值',  
   d_lastfollow_dealf_diff int COMMENT '最后一次跟进日期距成交/战败日期差值',  
   d_dealf_lastvisit_diff int COMMENT '成交/战败时间与全经销商处最后一次到店时间日期差值',  
   d_dealf_firvisit_diff int COMMENT '成交/战败时间与全经销商处首次到店时间日期差值',  
   d_lasttrail_dealf_diff int COMMENT '最后一次试乘试驾日期距最后成交/战败时间差值',  
   d_firleads_firvisit_diff int COMMENT '首次留资时间与首次到店时间日期差值',  
   d_leads_dtbt_ppt double COMMENT '留资经销商到店比例',   
   d_fircard_firvisit_diff int COMMENT '首次建卡时间距首次到店时间日期差值',  
   d_fir_dealfail_deal_diff int COMMENT '客户首次战败日期距成交时间天数',  
   d_deal_fail_ppt double COMMENT '战败比例',         
   d_last_activity_dealf_diff int COMMENT '最后一次参加活动时间距成交(战败)日期差值',  
   d_fir_activity_dealf_diff int COMMENT '首次参加活动时间距成交(战败)日期差值',  
   d_fir_order_leads_diff int COMMENT '首次下定时间与首次留资时间的日期差值',  
   d_fir_order_visit_diff int COMMENT '首次下定时间与首次到店时间的日期差值',  
   d_fir_order_trail_diff int COMMENT '首次下定时间与首次试乘试驾的日期差值',  
   c_is_app_user int COMMENT '是否注册APP',           
   c_brwose_count_d14 int COMMENT '过去14天浏览官网页面次数',  
   c_avg_browse_time_d14 int COMMENT '过去14天访问APP平均时长',  
   c_last_app_visit_time int COMMENT '最近一次访问APP时间',  
   c_app_point_ttl_d14 int COMMENT '过去14天取得积分总数',  
   c_app_point_ttl int COMMENT '总使用积分次数',         
   c_last_app_point_use_time int COMMENT '最近一次使用积分时间',  
   c_is_trail_user int COMMENT '是否试乘试驾',          
   c_trail_vel string COMMENT '试驾车系',             
   c_last_trail_vel string COMMENT '最近一次试驾车系',    
   c_deliver_vel string COMMENT '购买车系',
   c_visit_count int COMMENT '总到店次数',             
   c_visit_count_d14 int COMMENT '过去14天到店次数',     
   c_deal_time string COMMENT '成交日期',             
   c_last_trail_time int COMMENT '最近一次试驾日期',      
   c_last_lead_time int COMMENT '最近跟进日期',         
   c_trail_count_d14 int COMMENT '过去14天预约试乘试驾次数',  
   c_last_reach_platform string COMMENT '最近触达平台',  
   c_lead_sources string COMMENT '线索来源',          
   c_last_bbs_time int COMMENT '最近一次查看资讯论坛时间',    
   c_order_time string COMMENT 'APP下订日期',         
   c_last_sis_result string COMMENT '上次外呼结果',     
   c_open_link int COMMENT '是否打开短链',              
   c_last_visit_time string COMMENT '最近一次进店时间',   
   c_last_activity_time string COMMENT '最近参加活动日期',  
   c_create_card_count int COMMENT '跨店建卡数量',      
   c_cust_live_period int COMMENT '客户寿命(当前时间-客户最早留资时间)',  
   c_followup_ttl int COMMENT '历史跟进次数',           
   c_last_sis_time string COMMENT '上次外呼日期',       
   c_deal_fail_time string COMMENT '战败日期',        
   c_lead_model string COMMENT '留资车系',            
   c_fail_status int COMMENT '战败核实状态',            
   c_rx5_video_time int COMMENT 'RX5 MAX视频观看记录(APP端RX5 MAX视频观看类型)',  
   c_marvel_video_time int COMMENT 'MARVELX视频观看记录',  
   c_sex string COMMENT '性别',                     
   c_age string COMMENT '年龄',                     
   c_province string COMMENT '省份',                
   c_city string COMMENT '城市',                    
   c_last_lost_type int COMMENT '最近流失预警类型',       
   c_last_lost_time int COMMENT '最近流失预警日期',       
   c_roewe_brwose_homepage int COMMENT '是否浏览爱车展厅了解详情车型及配置页',  
   c_search_vel int COMMENT '是否搜索过车型关键词',         
   c_browse_model_info int COMMENT '是否观看过车型的资讯',  
   c_trail int COMMENT '是否预约过车型试驾',               
   c_make_deal int COMMENT '是否下订',                
   c_model int COMMENT '关注车系',                    
   c_register_time int COMMENT 'APP注册时间',         
   c_last_cust_base_time int COMMENT '最近一次建卡时间',  
   c_fail_real_result int COMMENT '战败核实结果',       
   c_app_browse_count_d14 int COMMENT '过去14天浏览APP次数',  
   h_goal string COMMENT '购车目的',                  
   h_loan int COMMENT '贷款概率',                     
   h_priority_car_type string COMMENT '(高优先级)购车喜好',  
   h_configuration_preference string,             
   h_priority_config_prefer string COMMENT '(高优先级)配置喜好,(动力偏好)',  
   h_priority_car_usage string COMMENT '购车用途',    
   h_client_focus string COMMENT '客户关注点',         
   h_models string COMMENT '车型偏好',                
   h_model_nums int COMMENT '关注竞品数量',             
   h_have_car int COMMENT '是否有车:1是,-1否,0空',       
   h_compete int COMMENT '本品竞争度:1低2中3高',          
   h_compete_car_num_d30 int COMMENT '30天关注竞品数量',  
   h_compete_car_num_d60 int COMMENT '60天关注竞品数量',  
   h_compete_car_num_d90 int COMMENT '90天关注竞品数量',  
   h_focusing_avg_diff int COMMENT '竞品关注度和名爵车型平均关注度的差值',  
   h_focusing_max_diff int COMMENT '竞品关注度和名爵车型最高关注度的差值',  
   h_func_prefer string COMMENT '功能偏好:functional_preference字段是否出现该词',  
   h_car_type_prefer string COMMENT '购车喜好',       
   h_config_prefer string COMMENT '单个竞品最大询价次数',   
   h_budget string COMMENT '购车预算',                
   h_displace int COMMENT '置换概率',                 
   h_level string COMMENT '级别',                   
   h_volume string COMMENT '排量',                  
   h_priority_change_car_and_buy int,             
   h_ttl_inquiry_time int COMMENT '所有竞品总询价次数',    
   h_max_inquiry_time int COMMENT '单个竞品车型最大询价次数',  
   h_produce_way string COMMENT '生产方式',           
   h_config_nums int COMMENT '配置标签数',             
   h_leads_create_time date COMMENT '留资时间',       
   s_firstleads_lastcard_call_count int COMMENT '首次留资时间和最后一次建卡时间之间的总外呼次数',  
   s_firstleads_firstcard_call_count int COMMENT '首次留资时间和首次建卡时间之间的总外呼次数',  
   s_avg_talk_length int COMMENT '平均通话时长（分钟）',    
   s_talk_connet_ppt float COMMENT '接通率',         
   s_first_call_result string COMMENT '首次通话结果',   
   d_fail_desc_list array<varchar(2000)> COMMENT '战败原因',  
   d_fail_time_list array<timestamp> COMMENT '战败时间',  
   d_cust_dealer_ids array<bigint> COMMENT '建卡经销商IDs',  
   m_last_visit_time timestamp COMMENT '最后一次到店日期',  
   m_focus_series_count int COMMENT '意向车型数量（来自oppor表）',  
   m_oppr_fail_count int COMMENT '战败次数',          
   m_cust_max_level string COMMENT '最大意向级别',      
   m_failed_called int COMMENT '是否被战败外呼',         
   m_lead_source_count int COMMENT '一二级线索来源数量',   
   m_last_lead_first_source string COMMENT '最后一次一级线索来源',  
   m_last_lead_second_source string COMMENT '最后一次二级线索来源'
)
PARTITIONED BY (pt STRING COMMENT "datetime")
ROW FORMAT SERDE 
	  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
WITH SERDEPROPERTIES ( 
	  'field.delim'='\t', 
	  'serialization.format'='\t') 
STORED AS INPUTFORMAT 
	  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
	  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
;

insert overwrite table marketing_modeling.app_big_wide_info_new partition (pt='20200907')
SELECT 
		a1.mobile,
		a1.fir_leads_time,               -- 首次留资时间
		a1.last_leads_time,               -- 最后留资时间
		a1.fir_sec_leads_diff,           -- 首次留资时间与二次留资时间日期差值
		a1.leads_count,                  -- 总留资次数
		a1.leads_channel,                --留资渠道
		a1.avg_leads_date,               -- 平均留资时间间隔
		a1.leads_channel_count,          -- 留资渠道总数
		a1.leads_dtbt_count,             -- 留资总经销商数
		a1.leads_dtbt_coincide,          -- 留资经销商重合度
		a1.leads_dtbt_level_1,           -- 一级经销商留资数量
		a1.leads_dtbt_level_2,           -- 二级经销商留资数量
		a1.cust_type,                    --客户属性 (个人或企业)
		a1.fir_card_time,                -- 首次建卡时间
		a1.last_card_time,               -- 首次建卡时间
		a1.card_ttl,                     -- 总建卡次数
		a1.cust_ids,                     -- 建卡ID数组
		a1.clue_issued_times,            -- 线索总下发次数 
		a1.fir_visit_time,               --	首次到店时间
		a1.fir_sec_visit_diff,           ---首次到店时间与二次到店时间日期差值
		a1.visit_dtbt_count,             --	到店总经销商数量
		a1.visit_ttl,                    --	总到店次数
		a1.avg_visit_date,               --平均到店时间间隔
		a1.dealer_ids,                   -- 经销商ID数组
		a1.avg_visit_dtbt_count,         --平均每个经销商处到店次数  
		a1.followup_ttl,                 --总跟进次数
		a1.fir_trail,                    ---首次试乘试驾时间
		a1.last_reservation_time,        -- 最后一次试驾预约试驾
		a1.trail_book_tll,               ---试乘试驾总预定次数
		a1.trail_attend_ttl,             ---试乘试驾总参与次数
		a1.trail_attend_ppt,              --试乘试驾参与率
		a1.leads_car_model_count,       --留资车型数量
		a1.leads_car_model_type,        --留资车型种类数
		a1.vel_series_ids,              --车型IDs
		a1.series_types,                --车型种类IDs
		a1.rw_car_model_count,          --荣威留资车型数量
		a1.deal_time,                   -- 首次成交时间
		a1.last_deal_time,                   -- 最后成交时间
		a1.deal_ttl,                    -- 总成交次数
		a1.deal_car_ttl,                -- 两年内总购买车辆数
		a1.fir_dealfail_d,               -- 首次战败时间
		a1.last_dealfail_d,             -- 最后战败时间
		a1.is_deposit_order,             --是否下定
		a1.deal_flag,                     -- 是否成交 1-成交，0-战败，null-跟进
		a1.leads_pool_status,
		a1.dealf_succ_firvisit_diff,    --成交时间与成交经销商处首次到店时间日期差值
		a1.dealf_succ_lastvisit_diff,   --成交时间与成交经销商处最后一次到店时间日期差值
		a1.avg_fir_sec_visit_diff,      --平均首次到店时间与二次到店时间日期差值
		a1.fircard_firvisit_dtbt_diff,  --建卡时间距首次到店时间日期差值(每个经销商有个value)
		a1.avg_fircard_firvisit_diff,     --平均建卡时间距首次到店时间日期差值
		a1.avg_firleads_firvisit_diff,    --平均首次留资时间距首次到店时间日期差值
		a1.fir_activity_time,             -- 首次活动时间
		a1.activity_ttl,                   -- 总活动次数
		a1.natural_visit,                  --自然到店
		a1.fir_leads_deal_diff_y,          -- 上次成交时间距首次留资时间年数	
		a1.has_deliver_history,         --之前是否荣威/名爵车主
		a1.trail_count_d30,           --过去30天总试驾数
		a1.trail_count_d90,            --过去30天总试驾数
		a1.visit_count_d15,            --过去15天总到店次数
		a1.visit_count_d30,            --过去30天总到店次数
		a1.visit_count_d90,             --过去90天总到店次数
		a1.followup_d7,            --过去7天跟进次数
		a1.followup_d15,           --过去15天跟进次数
		a1.followup_d30,           --过去30天跟进次数
		a1.followup_d60,           --过去60天跟进次数
		a1.followup_d90,            --过去90天跟进次数
		a1.activity_count_d15,     --过去15天参加活动次数
		a1.activity_count_d30,     --过去30天参加活动次数
		a1.activity_count_d60,     --过去60天参加活动次数
		a1.activity_count_d90,      --过去90天参加活动次数	
		a1.firlead_dealf_diff,        --- 首次留资时间距最后成交/战败时间差值
		a1.lastlead_dealf_diff,       --- 最后一次留资时间距最后成交/战败时间差值
		a1.last_dfail_dealf_diff,	  --- 客户最后一次战败日期距成交/战败日期天数
		a1.firfollow_dealf_diff,      -- 首次跟进日期距成交/战败时间差值
		a1.lastfollow_dealf_diff,	  --- 最后一次跟进日期距成交/战败日期差值
		a1.dealf_lastvisit_diff,	  -- 成交/战败时间与全经销商处最后一次到店时间日期差值
		a1.dealf_firvisit_diff,	      --- 成交/战败时间与全经销商处首次到店时间日期差值
		a1.lasttrail_dealf_diff,	  --- 最后一次试乘试驾日期距最后成交/战败时间差值
		a1.firleads_firvisit_diff,	  ---首次留资时间与首次到店时间日期差值
		a1.leads_dtbt_ppt,            -- 留资经销商到店比例
		a1.fircard_firvisit_diff,	  -- 首次建卡时间距首次到店时间日期差值
		a1.fir_dealfail_deal_diff,	  -- 客户首次战败日期距成交时间天数
		a1.deal_fail_ppt,	          ---战败比例
		a1.last_activity_dealf_diff,  --最后一次参加活动时间距成交(战败)日期差值
		a1.fir_activity_dealf_diff,    --首次参加活动时间距成交(战败)日期差值
		a1.fir_order_leads_diff,	--首次下定时间与首次留资时间的日期差值
		a1.fir_order_visit_diff,	--首次下定时间与首次到店时间的日期差值
		a1.fir_order_trail_diff	    --首次下定时间与首次试乘试驾的日期差值  
		,a2.is_app_user                       --'是否注册APP'
		,a2.brwose_count_d14                  --'过去14天浏览官网页面次数'
		,a2.avg_browse_time_d14               --'过去14天访问APP平均时长'
		,a2.last_app_visit_time               --'最近一次访问APP时间'
		,a2.app_point_ttl_d14                 --'过去14天取得积分总数'
		,a2.app_point_ttl                     --'总使用积分次数'
		,a2.last_app_point_use_time           --'最近一次使用积分时间'
		,a2.is_trail_user                     --'是否试乘试驾'
		,a2.trail_vel                         --'试驾车系'
		,a2.last_trail_vel                    --'最近一次试驾车系'
		,a2.deliver_vel                       --'购买车系'
		,a2.visit_count                       --'总到店次数'
		,a2.visit_count_d14                   --'过去14天到店次数'
		,a2.deal_time as deal_time_cdp                       --'成交日期'
		,a2.last_trail_time                   --'最近一次试驾日期'
		,a2.last_lead_time                    --'最近跟进日期'
		,a2.trail_count_d14                   --'过去14天预约试乘试驾次数'
		,a2.last_reach_platform               --'最近触达平台'
		,a2.lead_sources                      --'线索来源'
		,a2.last_bbs_time                     --'最近一次查看资讯论坛时间'
		,a2.order_time                        --'APP下订日期'
		,a2.last_sis_result                   --'上次外呼结果'
		,a2.open_link                         --'是否打开短链'
		,a2.last_visit_time as last_visit_time_cdp                  --'最近一次进店时间'
		,a2.last_activity_time                --'最近参加活动日期'
		,a2.create_card_count                 --'跨店建卡数量'
		,a2.cust_live_period                  --'客户寿命(当前时间-客户最早留资时间)'
		,a2.followup_ttl as followup_ttl_cdp                     --'历史跟进次数'
		,a2.last_sis_time                     --'上次外呼日期'
		,a2.deal_fail_time                    --'战败日期'
		,a2.lead_model                        --'留资车系'
		,a2.fail_status                       --'战败核实状态'
		,a2.rx5_video_time                    --'RX5 MAX视频观看记录(APP端RX5 MAX视频观看类型)'
		,a2.marvel_video_time                 --'MARVELX视频观看记录'
		,a2.sex                               --'性别'
		,a2.age                               --'年龄'
		,a2.province                          --'省份'
		,a2.city                              --'城市'
		,a2.last_lost_type                    --'最近流失预警类型'
		,a2.last_lost_time                    --'最近流失预警日期'
		,a2.roewe_brwose_homepage             --'是否浏览爱车展厅了解详情车型及配置页'
		,a2.search_vel                        --'是否搜索过车型关键词'
		,a2.browse_model_info                 --'是否观看过车型的资讯'
		,a2.trail                             --'是否预约过车型试驾'
		,a2.make_deal                         --'是否下订'
		,a2.model                             --'关注车系'
		,a2.register_time                     --'APP注册时间'
		,a2.last_cust_base_time               --'最近一次建卡时间'
		,a2.fail_real_result                  --'战败核实结果'
		,a2.app_browse_count_d14              --'过去14天浏览APP次数'	
		,a3.goal                    
		,a3.loan                    
		,a3.priority_car_type       
		,a3.configuration_preference
		,a3.priority_config_prefer  
		,a3.priority_car_usage      
		,a3.client_focus            
		,a3.models                  
		,a3.model_nums              
		,a3.have_car                
		,a3.compete                 
		,a3.compete_car_num_d30     
		,a3.compete_car_num_d60     
		,a3.compete_car_num_d90     
		,a3.focusing_avg_diff       
		,a3.focusing_max_diff       
		,a3.func_prefer             
		,a3.car_type_prefer         
		,a3.config_prefer           
		,a3.budget                  
		,a3.displace                
		,a3.level                   
		,a3.volume                  
		,a3.priority_change_car_and_buy
		,a3.ttl_inquiry_time        
		,a3.max_inquiry_time        
		,a3.produce_way   
		,a3.config_nums
		,a3.leads_create_time   
		,a4.firstleads_lastcard_call_count
		,a4.firstleads_firstcard_call_count
		,a4.avg_talk_length
		,a4.talk_connet_ppt
		,a4.first_call_result
		,a1.fail_desc_list
		,a1.fail_time_list
		,a1.cust_dealer_ids
		,a1.last_visit_time
		,a1.leads_car_model_count as car_model_count
		,a1.deal_fail_times
		,a1.max_cust_level
		,a4.failed_call_client
		,a1.source_count
		,a1.latest_first_source_name
		,a1.latest_second_source_name
	FROM
		marketing_modeling.app_dlm_wide_info a1
	left join 
		marketing_modeling.app_cdp_wide_info a2 on a1.mobile = a2.id 
	left join
		marketing_modeling.app_autohome_wide_info a3 on a1.mobile = a3.id
	left join 
		marketing_modeling.app_sis_wide_info a4 on a1.mobile = a4.mobile
	WHERE
		a1.mobile regexp "^[1][3-9][0-9]{9}$"

