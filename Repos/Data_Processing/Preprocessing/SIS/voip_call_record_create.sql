USE dtwarehouse;
DROP TABLE IF EXISTS dtwarehouse.voip_call_record;
CREATE TABLE IF NOT EXISTS dtwarehouse.voip_call_record (
  `id` bigint  COMMENT '主键',
  `type` int COMMENT '通话类别，1：呼入；2：呼出',
  `org_flg` int COMMENT '是否组内转入，0：否；1：是',
  `org_code` string COMMENT '售前售后组织编号, 01售前, 02售后',
  `user_id` int COMMENT '坐席ID',
  `agent_id` string COMMENT '座席工号',
  `call_no` string COMMENT '来电/外呼号码',
  `call_no_prefix` string COMMENT '来电/外呼号码 包含出局前缀',
  `transfer_no` string COMMENT '转移号码',
  `third_party_no` string COMMENT '三方号码',
  `cust_id` int COMMENT '客户ID',
  `oper_name` string COMMENT '操作业务类型名称(咨询,投诉,道路救援,差评工单,送电超时等)',
  `oper_type` int COMMENT '操作类型 枚举 数据字典表：TYPE_NO=CALLOPERTYPE',
  `oper_id` string COMMENT '操作ID(工单ID/外呼任务ID/其他操作业务ID)',
  `cust_name` string COMMENT '客户姓名',
  `connid` string COMMENT '呼叫ID',
  `extension` string COMMENT '座席分机号',
  `begin_time` timestamp  COMMENT '通话开始时间',
  `end_time` timestamp COMMENT '通话结束时间',
  `outbound_config_code` string COMMENT '通话结果',
  `is_connected` int COMMENT '是否接通：0 未接通，1 已接通',
  `talk_length` int COMMENT '通话时长单位：豪秒',
  `remark` string COMMENT '备注',
  `content` string COMMENT '话务小结',
  `manyidu_dispatch_time` timestamp COMMENT '满意度评分下发时间',
  `manyidu_end_time` timestamp COMMENT '满意度评分时间',
  `manyidu` string COMMENT '满意度评分(1:非常满意,2:满意,3:一般,4:不满意,5:非常不满意)',
  `ib_select_key` string COMMENT 'IB呼入按键选项',
  `gongyue_select_key` string COMMENT '宅捷修公约按键选项',
  `create_by` string COMMENT '创建人',
  `create_time` timestamp COMMENT '创建时间',
  `update_by` string COMMENT '更新人',
  `update_time` timestamp COMMENT '更新时间',
  `o_cac_id` bigint COMMENT 'CAC t_call_record表id'
 )
PARTITIONED BY (pt STRING COMMENT "datetime")
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':'
STORED AS TEXTFILE;


