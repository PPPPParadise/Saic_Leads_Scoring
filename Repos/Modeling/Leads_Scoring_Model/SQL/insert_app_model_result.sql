set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
set tez.queue.name=${queuename};

insert overwrite table marketing_modeling.app_model_result partition (pt='${pt}') 
SELECT * FROM marketing_modeling.tmp_app_model_result
;