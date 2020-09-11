### 1. 结构
```SQL
├──README.md
├──config.ini               #数据处理流程中的全局配置文件，主要配置时间范围days=N天，queuename=malg
├──dos2unix.sh              #每次从git拉全量代码后需要执行下这个脚本，更改shell脚本的文件格式
├──run.sh                   #本地测试用的脚本，目前不需要了
├──Speech                   #语音
	|                       #暂时无数据
├──Timeline                 #时光轴
    |──app_timeline_behavior_create.sql   #建表脚本
	|──dmp_user_info.sql                
	|──gio_finance_count.sql
	|──timeline_behavior.sql
	|──total_browse_time.sql
	|──run.sh
├──UserProfile                           #
    |──userprofile_create.sql            #建表脚本
	|──userprofile.sql                   #处理脚本 
	|──run.sh
├──WEB
    |──scripts
    |──sqls
├──WideTable
    |──create_scripts
		|──app_big_wide_info_create.sql   #大宽表建表语句
	|──sql
		|──app_big_wide_info.sql          #join小宽表数据到大宽表
	|──run.sh
├──WideTable_AutoHome                     #汽车之家
    |──autohome.py                        #处理脚本
	|──autohome_processing.sql            #去重脚本
	|──run.sh
	|──runsql.sh
├──WideTable_CDP                          #CDP小宽表
    |──create_scripts                     #建表脚本
    |──sql
	    |──cdp_middle_tb.sql              #预处理
		|──cdp_mapping_tb.sql             #2次处理
		|──cdp_final_tb.sql               #生成数据
		|──run.sh
├──WideTable_DLM                          #DLM数据处理
    |──README.md                          #该文件中有详细描述
	
├──WideTable_SIS                          #SIS外呼数据
    |──tmp_sis_task_item_cleansing_1.sql   #数据清洗1
	|──tmp_sis_task_item_cleansing_2.sql   #数据清洗2
	|──tmp_sis_task_item_processing.sql    #生成数据
	|──run.sh
	|──run_cleansing.sh           

   