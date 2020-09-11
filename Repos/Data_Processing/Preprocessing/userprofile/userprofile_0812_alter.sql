alter table marketing_modeling.edw_mkt_userprofile add columns (device_id string comment 'device_id');
alter table marketing_modeling.edw_mkt_userprofile add columns (union_id string comment 'union_id');
alter table marketing_modeling.edw_mkt_userprofile add columns (cookies string comment 'cookies');
alter table marketing_modeling.edw_mkt_userprofile add columns (fail_reason_fir_level array<string> comment '一级战败原因');
alter table marketing_modeling.edw_mkt_userprofile add columns (fail_reason_sec_level array<string> comment '二级战败原因');
	