DROP TABLE IF EXISTS marketing_modeling.app_big_wide_info;
CREATE TABLE IF NOT EXISTS marketing_modeling.app_big_wide_info (
	mobile                   string               comment   '用户唯一ID，mobile'
	,d_fir_leads_time          timestamp          comment     '首次留资时间'
	,d_last_leads_time          timestamp          comment     '最后留资时间'
	,d_fir_sec_leads_diff      int                comment     '首次留资时间与二次留资时间日期差值'
	,d_leads_count             bigint             comment     '总留资次数'
	,d_leads_channel           array<int>         comment     '留资渠道'
	,d_avg_leads_date          double             comment     '平均留资时间间隔'
	,d_leads_channel_count     bigint             comment     '留资渠道总数'
	,d_leads_dtbt_count        bigint             comment     '留资总经销商数'
	,d_leads_dtbt_coincide     double             comment     '留资经销商重合度'
	,d_leads_dtbt_level_1      bigint             comment     '一级经销商留资数量'
	,d_leads_dtbt_level_2      bigint             comment     '二级经销商留资数量'
	,d_cust_type               int                comment     '客户属性 (个人或企业)'
	,d_fir_card_time           timestamp          comment     '首次建卡时间'
	,d_last_card_time          timestamp          comment     '首次建卡时间'
	,d_card_ttl                bigint             comment     '总建卡次数'
	,d_cust_ids                array<bigint>      comment     '建卡ID数组'
	,d_clue_issued_times       bigint             comment     '线索总下发次数'
	,d_fir_visit_time          timestamp          comment     '首次到店时间'
	,d_fir_sec_visit_diff      int                comment     '首次到店时间与二次到店时间日期差值'
	,d_visit_dtbt_count        bigint             comment     '到店总经销商数量'
	,d_visit_ttl               bigint             comment     '总到店次数'
	,d_avg_visit_date          double             comment     '平均到店时间间隔'
	,d_dealer_ids              array<bigint>      comment     ' 经销商ID数组'
	,d_avg_visit_dtbt_count    double             comment     '平均每个经销商处到店次数'  
	,d_followup_ttl            bigint             comment     '总跟进次数'
	,d_fir_trail               timestamp          comment     '-首次试乘试驾时间'
	,d_last_reservation_time   timestamp          comment     ' 最后一次试驾预约试驾'
	,d_trail_book_tll          bigint             comment     '-试乘试驾总预定次数'
	,d_trail_attend_ttl        bigint             comment     '-试乘试驾总参与次数'
	,d_trail_attend_ppt        double             comment     '-试乘试驾参与率'
	,d_leads_car_model_count   bigint             comment     '留资车型数量'
	,d_leads_car_model_type    bigint             comment     '留资车型种类数'
	,d_vel_series_ids          array<bigint>      comment     '车型IDs'
	,d_series_types            array<string>      comment     '车型种类IDs'
	,d_rw_car_model_count      bigint             comment     '荣威留资车型数量'
	,d_deal_time               timestamp          comment     ' 首次成交时间'
	,d_last_deal_time          timestamp          comment     ' 最后成交时间'
	,d_deal_ttl                bigint             comment     ' 总成交次数'
	,d_deal_car_ttl            bigint             comment     ' 两年内总购买车辆数'
	,d_fir_dealfail_d          timestamp          comment     '- 首次战败时间'
	,d_last_dealfail_d         timestamp          comment     ' 最后战败时间'
	,d_is_deposit_order        int                comment     '-是否下定'
	,d_deal_flag               int                comment     '- 是否成交 1-成交，0-战败，null-跟进'
	,d_leads_pool_status       string                comment     '状态：001-未下发；002-培育；003-跟进'
	,d_dealf_succ_firvisit_diff        double     comment     '成交时间与成交经销商处首次到店时间日期差值'
	,d_dealf_succ_lastvisit_diff       double     comment     '成交时间与成交经销商处最后一次到店时间日期差值'
	,d_avg_fir_sec_visit_diff  double             comment     '平均首次到店时间与二次到店时间日期差值'
	,d_fircard_firvisit_dtbt_diff      array<int>    comment     '建卡时间距首次到店时间日期差值(每个经销商有个value)'
	,d_avg_fircard_firvisit_diff       double     comment     '平均建卡时间距首次到店时间日期差值'
	,d_avg_firleads_firvisit_diff      double     comment     '平均首次留资时间距首次到店时间日期差值'
	,d_fir_activity_time       timestamp          comment     '首次活动时间'
	,d_activity_ttl            bigint             comment     '总活动次数'
	,d_natural_visit           string             comment     '自然到店'
	,d_fir_leads_deal_diff_y   double             comment     '上次成交时间距首次留资时间年数'	
	,d_has_deliver_history     int                comment     '之前是否荣威/名爵车主'
	,d_trail_count_d30         bigint             comment     '过去30天总试驾数'
	,d_trail_count_d90         bigint             comment     '过去30天总试驾数'
	,d_visit_count_d15         bigint             comment     '过去15天总到店次数'
	,d_visit_count_d30         bigint             comment     '过去30天总到店次数'
	,d_visit_count_d90         bigint             comment     '过去90天总到店次数'
	,d_followup_d7             bigint             comment     '过去7天跟进次数'
	,d_followup_d15            bigint             comment     '过去15天跟进次数'
	,d_followup_d30            bigint             comment     '过去30天跟进次数'
	,d_followup_d60            bigint             comment     '过去60天跟进次数'
	,d_followup_d90            bigint             comment     '过去90天跟进次数'
	,d_activity_count_d15      bigint             comment     '过去15天参加活动次数'
	,d_activity_count_d30      bigint             comment     '过去30天参加活动次数'
	,d_activity_count_d60      bigint             comment     '过去60天参加活动次数'
	,d_activity_count_d90      bigint             comment     '过去90天参加活动次数'	
	,d_firlead_dealf_diff      int                comment     '首次留资时间距最后成交/战败时间差值'
	,d_lastlead_dealf_diff     int                comment     '最后一次留资时间距最后成交/战败时间差值'
	,d_last_dfail_dealf_diff   int                comment     '客户最后一次战败日期距成交/战败日期天数'
	,d_firfollow_dealf_diff    int                comment     '首次跟进日期距成交/战败时间差值'
	,d_lastfollow_dealf_diff   int                comment     '最后一次跟进日期距成交/战败日期差值'
	,d_dealf_lastvisit_diff    int                comment     '成交/战败时间与全经销商处最后一次到店时间日期差值'
	,d_dealf_firvisit_diff     int                comment     '成交/战败时间与全经销商处首次到店时间日期差值'
	,d_lasttrail_dealf_diff    int                comment     '最后一次试乘试驾日期距最后成交/战败时间差值'
	,d_firleads_firvisit_diff  int                comment     '首次留资时间与首次到店时间日期差值'
	,d_leads_dtbt_ppt          double             comment     '留资经销商到店比例'
	,d_fircard_firvisit_diff   int                comment     '首次建卡时间距首次到店时间日期差值'
	,d_fir_dealfail_deal_diff  int                comment     '客户首次战败日期距成交时间天数'
	,d_deal_fail_ppt           double             comment     '战败比例'
	,d_last_activity_dealf_diff        int        comment     '最后一次参加活动时间距成交(战败)日期差值'
	,d_fir_activity_dealf_diff int                comment     '首次参加活动时间距成交(战败)日期差值'
	,d_fir_order_leads_diff    int                comment     '首次下定时间与首次留资时间的日期差值'
	,d_fir_order_visit_diff    int                comment     '首次下定时间与首次到店时间的日期差值'
	,d_fir_order_trail_diff    int                comment     '首次下定时间与首次试乘试驾的日期差值'
	
	,c_is_app_user             int                 comment   '是否注册APP'
	,c_brwose_count_d14        int                 comment   '过去14天浏览官网页面次数'
	,c_avg_browse_time_d14     int                 comment   '过去14天访问APP平均时长'
	,c_last_app_visit_time     int                 comment   '最近一次访问APP时间'
	,c_app_point_ttl_d14       int                 comment   '过去14天取得积分总数'
	,c_app_point_ttl           int                 comment   '总使用积分次数'
	,c_last_app_point_use_time int                 comment   '最近一次使用积分时间'
	,c_is_trail_user           int                 comment   '是否试乘试驾'
	,c_trail_vel               string              comment   '试驾车系'
	,c_last_trail_vel          string              comment   '最近一次试驾车系'
	,c_deliver_vel             string              comment   '购买车系'
	,c_visit_count             int                 comment   '总到店次数'
	,c_visit_count_d14         int                 comment   '过去14天到店次数'
	,c_deal_time               string              comment   '成交日期'
	,c_last_trail_time         int                 comment   '最近一次试驾日期'
	,c_last_lead_time          int                 comment   '最近跟进日期'
	,c_trail_count_d14         int                 comment   '过去14天预约试乘试驾次数'
	,c_last_reach_platform     string              comment   '最近触达平台'
	,c_lead_sources            string              comment   '线索来源'
	,c_last_bbs_time           int                 comment   '最近一次查看资讯论坛时间'
	,c_order_time              string              comment   'APP下订日期'
	,c_last_sis_result         string              comment   '上次外呼结果'
	,c_open_link               int                 comment   '是否打开短链'
	,c_last_visit_time         string              comment   '最近一次进店时间'
	,c_last_activity_time      string              comment   '最近参加活动日期'
	,c_create_card_count       int                 comment   '跨店建卡数量'
	,c_cust_live_period        int                 comment   '客户寿命(当前时间-客户最早留资时间)'
	,c_followup_ttl            int                 comment   '历史跟进次数'
	,c_last_sis_time           string              comment   '上次外呼日期'
	,c_deal_fail_time          string              comment   '战败日期'
	,c_lead_model              string              comment   '留资车系'
	,c_fail_status             int                 comment   '战败核实状态'
	,c_rx5_video_time          int                 comment   'RX5 MAX视频观看记录(APP端RX5 MAX视频观看类型)'
	,c_marvel_video_time       int                 comment   'MARVELX视频观看记录'
	,c_sex                     string              comment   '性别'
	,c_age                     string              comment   '年龄'
	,c_province                string              comment   '省份'
	,c_city                    string              comment   '城市'
	,c_last_lost_type          int                 comment   '最近流失预警类型'
	,c_last_lost_time          int                 comment   '最近流失预警日期'
	,c_roewe_brwose_homepage   int                 comment   '是否浏览爱车展厅了解详情车型及配置页'
	,c_search_vel              int                 comment   '是否搜索过车型关键词'
	,c_browse_model_info       int                 comment   '是否观看过车型的资讯'
	,c_trail                   int                 comment   '是否预约过车型试驾'
	,c_make_deal               int                 comment   '是否下订'
	,c_model                   int                 comment   '关注车系'
	,c_register_time           int                 comment   'APP注册时间'
	,c_last_cust_base_time     int                 comment   '最近一次建卡时间'
	,c_fail_real_result        int                 comment   '战败核实结果'
	,c_app_browse_count_d14    int                 comment   '过去14天浏览APP次数'

	,h_goal                    string               comment    '购车目的'
	,h_loan                    int       	       comment    '贷款概率'  
	,h_priority_car_type       string               comment    '(高优先级)购车喜好'
	,h_configuration_preference        string       
	,h_priority_config_prefer  string               comment    '(高优先级)配置喜好,(动力偏好)'
	,h_priority_car_usage      string               comment    '购车用途'
	,h_client_focus            string               comment    '客户关注点'  
	,h_models                  string               comment    '车型偏好' 
	,h_model_nums              int                  comment    '关注竞品数量'    
	,h_have_car                int              	comment    '是否有车:1是,-1否,0空'
	,h_compete                 int              	comment    '本品竞争度:1低2中3高'
	,h_compete_car_num_d30     int                  comment    '30天关注竞品数量'  
	,h_compete_car_num_d60     int                  comment    '60天关注竞品数量'   
	,h_compete_car_num_d90     int                  comment    '90天关注竞品数量'     
	,h_focusing_avg_diff       int                  comment    '竞品关注度和名爵车型平均关注度的差值'
	,h_focusing_max_diff       int                  comment    '竞品关注度和名爵车型最高关注度的差值'
	,h_func_prefer             string	            comment    '功能偏好:functional_preference字段是否出现该词'
	,h_car_type_prefer         string               comment    '购车喜好'
	,h_config_prefer           string               comment    '单个竞品最大询价次数'
	,h_budget                  string               comment    '购车预算'
	,h_displace                int					comment    '置换概率' 
	,h_level                   string				comment    '级别'
	,h_volume                  string				comment    '排量'
	,h_priority_change_car_and_buy     int
	,h_ttl_inquiry_time        int                  comment    '所有竞品总询价次数'
	,h_max_inquiry_time        int                  comment    '单个竞品车型最大询价次数'
	,h_produce_way             string               comment    '生产方式'
	,h_config_nums             int                  comment    '配置标签数'
	,h_leads_create_time       date                 comment    '留资时间'
	
	,s_firstleads_lastcard_call_count        int      comment    '首次留资时间和最后一次建卡时间之间的总外呼次数'
	,s_firstleads_firstcard_call_count       int      comment    '首次留资时间和首次建卡时间之间的总外呼次数'
	,s_avg_talk_length             int                comment    '平均通话时长（分钟）'
	,s_talk_connet_ppt             float              comment    '接通率'
	,s_first_call_result           string             comment    '首次通话结果'
)
PARTITIONED BY (pt STRING COMMENT "datetime")
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':'
STORED AS TEXTFILE;
;
	