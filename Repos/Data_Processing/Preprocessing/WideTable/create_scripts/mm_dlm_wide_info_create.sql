DROP TABLE IF EXISTS marketing_modeling.mm_dlm_wide_info;
CREATE TABLE IF NOT EXISTS marketing_modeling.mm_dlm_wide_info (
	mobile                   string               comment   '用户唯一ID，mobile'
	,fir_leads_time          timestamp          comment     '首次留资时间'
	,last_leads_time          timestamp          comment     '最后留资时间'
	,fir_sec_leads_diff      int                comment     '首次留资时间与二次留资时间日期差值'
	,leads_count             bigint             comment     '总留资次数'
	,leads_channel           array<int>         comment     '留资渠道'
	,avg_leads_date          double             comment     '平均留资时间间隔'
	,leads_channel_count     bigint             comment     '留资渠道总数'
	,leads_dtbt_count        bigint             comment     '留资总经销商数'
	,leads_dtbt_coincide     double             comment     '留资经销商重合度'
	,leads_dtbt_level_1      bigint             comment     '一级经销商留资数量'
	,leads_dtbt_level_2      bigint             comment     '二级经销商留资数量'
	,cust_type               int                comment     '客户属性 (个人或企业)'
	,fir_card_time           timestamp          comment     '首次建卡时间'
	,last_card_time          timestamp          comment     '首次建卡时间'
	,card_ttl                bigint             comment     '总建卡次数'
	,cust_ids                array<bigint>      comment     '建卡ID数组'
	,clue_issued_times       bigint             comment     '线索总下发次数'
	,fir_visit_time          timestamp          comment     '首次到店时间'
	,fir_sec_visit_diff      int                comment     '首次到店时间与二次到店时间日期差值'
	,visit_dtbt_count        bigint             comment     '到店总经销商数量'
	,visit_ttl               bigint             comment     '总到店次数'
	,avg_visit_date          double             comment     '平均到店时间间隔'
	,dealer_ids              array<bigint>      comment     ' 经销商ID数组'
	,avg_visit_dtbt_count    double             comment     '平均每个经销商处到店次数'  
	,followup_ttl            bigint             comment     '总跟进次数'
	,fir_trail               timestamp          comment     '-首次试乘试驾时间'
	,last_reservation_time   timestamp          comment     ' 最后一次试驾预约试驾'
	,trail_book_tll          bigint             comment     '-试乘试驾总预定次数'
	,trail_attend_ttl        bigint             comment     '-试乘试驾总参与次数'
	,trail_attend_ppt        double             comment     '-试乘试驾参与率'
	,leads_car_model_count   bigint             comment     '留资车型数量'
	,leads_car_model_type    bigint             comment     '留资车型种类数'
	,vel_series_ids          array<bigint>      comment     '车型IDs'
	,series_types            array<string>      comment     '车型种类IDs'
	,rw_car_model_count      bigint             comment     '荣威留资车型数量'
	,deal_time               timestamp          comment     ' 首次成交时间'
	,last_deal_time          timestamp          comment     ' 最后成交时间'
	,deal_ttl                bigint             comment     ' 总成交次数'
	,deal_car_ttl            bigint             comment     ' 两年内总购买车辆数'
	,fir_dealfail_d          timestamp          comment     '- 首次战败时间'
	,last_dealfail_d         timestamp          comment     ' 最后战败时间'
	,is_deposit_order        int                comment     '-是否下定'
	,deal_flag               int                comment     '- 是否成交 1-成交，0-战败，null-跟进'
	,dealf_succ_firvisit_diff        double     comment     '成交时间与成交经销商处首次到店时间日期差值'
	,dealf_succ_lastvisit_diff       double     comment     '成交时间与成交经销商处最后一次到店时间日期差值'
	,avg_fir_sec_visit_diff  double             comment     '平均首次到店时间与二次到店时间日期差值'
	,fircard_firvisit_dtbt_diff      array<int>    comment     '建卡时间距首次到店时间日期差值(每个经销商有个value)'
	,avg_fircard_firvisit_diff       double     comment     '平均建卡时间距首次到店时间日期差值'
	,avg_firleads_firvisit_diff      double     comment     '平均首次留资时间距首次到店时间日期差值'
	,fir_activity_time       timestamp          comment     '首次活动时间'
	,activity_ttl            bigint             comment     '总活动次数'
	,natural_visit           string             comment     '自然到店'
	,fir_leads_deal_diff_y   double             comment     '上次成交时间距首次留资时间年数'	
	,has_deliver_history     int                comment     '之前是否荣威/名爵车主'
	,trail_count_d30         bigint             comment     '过去30天总试驾数'
	,trail_count_d90         bigint             comment     '过去30天总试驾数'
	,visit_count_d15         bigint             comment     '过去15天总到店次数'
	,visit_count_d30         bigint             comment     '过去30天总到店次数'
	,visit_count_d90         bigint             comment     '过去90天总到店次数'
	,followup_d7             bigint             comment     '过去7天跟进次数'
	,followup_d15            bigint             comment     '过去15天跟进次数'
	,followup_d30            bigint             comment     '过去30天跟进次数'
	,followup_d60            bigint             comment     '过去60天跟进次数'
	,followup_d90            bigint             comment     '过去90天跟进次数'
	,activity_count_d15      bigint             comment     '过去15天参加活动次数'
	,activity_count_d30      bigint             comment     '过去30天参加活动次数'
	,activity_count_d60      bigint             comment     '过去60天参加活动次数'
	,activity_count_d90      bigint             comment     '过去90天参加活动次数'	
	,firlead_dealf_diff      int                comment     '首次留资时间距最后成交/战败时间差值'
	,lastlead_dealf_diff     int                comment     '最后一次留资时间距最后成交/战败时间差值'
	,last_dfail_dealf_diff   int                comment     '客户最后一次战败日期距成交/战败日期天数'
	,firfollow_dealf_diff    int                comment     '首次跟进日期距成交/战败时间差值'
	,lastfollow_dealf_diff   int                comment     '最后一次跟进日期距成交/战败日期差值'
	,dealf_lastvisit_diff    int                comment     '成交/战败时间与全经销商处最后一次到店时间日期差值'
	,dealf_firvisit_diff     int                comment     '成交/战败时间与全经销商处首次到店时间日期差值'
	,lasttrail_dealf_diff    int                comment     '最后一次试乘试驾日期距最后成交/战败时间差值'
	,firleads_firvisit_diff  int                comment     '首次留资时间与首次到店时间日期差值'
	,leads_dtbt_ppt          double             comment     '留资经销商到店比例'
	,fircard_firvisit_diff   int                comment     '首次建卡时间距首次到店时间日期差值'
	,fir_dealfail_deal_diff  int                comment     '客户首次战败日期距成交时间天数'
	,deal_fail_ppt           double             comment     '战败比例'
	,last_activity_dealf_diff        int        comment     '最后一次参加活动时间距成交(战败)日期差值'
	,fir_activity_dealf_diff int                comment     '首次参加活动时间距成交(战败)日期差值'
	,fir_order_leads_diff    int                comment     '首次下定时间与首次留资时间的日期差值'
	,fir_order_visit_diff    int                comment     '首次下定时间与首次到店时间的日期差值'
	,fir_order_trail_diff    int                comment     '首次下定时间与首次试乘试驾的日期差值'
)
PARTITIONED BY (pt STRING COMMENT "datetime")
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':'
STORED AS TEXTFILE;
	