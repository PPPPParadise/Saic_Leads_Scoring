set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;

insert overwrite table marketing_modeling.mm_big_wide_info partition (pt='${pt}')
SELECT 
	coalesce(a1.mobile,a2.id,a3.id) as mobile,
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
	,a2.deal_time                         --'成交日期'
	,a2.last_trail_time                   --'最近一次试驾日期'
	,a2.last_lead_time                    --'最近跟进日期'
	,a2.trail_count_d14                   --'过去14天预约试乘试驾次数'
	,a2.last_reach_platform               --'最近触达平台'
	,a2.lead_sources                      --'线索来源'
	,a2.last_bbs_time                     --'最近一次查看资讯论坛时间'
	,a2.order_time                        --'APP下订日期'
	,a2.last_sis_result                   --'上次外呼结果'
	,a2.open_link                         --'是否打开短链'
	,a2.last_visit_time                   --'最近一次进店时间'
	,a2.last_activity_time                --'最近参加活动日期'
	,a2.create_card_count                 --'跨店建卡数量'
	,a2.cust_live_period                  --'客户寿命(当前时间-客户最早留资时间)'
	,a2.followup_ttl                      --'历史跟进次数'
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
	
FROM
	marketing_modeling.mm_dlm_behavior_wide_info a1
left join 
	marketing_modeling.mm_cdp_user_behavior a2 on a1.mobile = a2.id 
left join
	marketing_modeling.mm_autohome_behavior a3 on a1.mobile = a3.id
left join 
	marketing_modeling.mm_sis_wide_info a4 on a1.mobile = a4.mobile
;