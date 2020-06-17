#!/bin/bash
pt=20200601
echo "===============================SIS task_item cleansing 1"
hive --hivevar pt=$pt -f sis_tmp_task_item_cleansing_1.sql 
echo "===============================SIS task_item cleansing 2"
hive --hivevar pt=$pt -f sis_tmp_task_item_cleansing_1.sql
echo "===============================SIS task_item processing"
hive --hivevar pt=$pt -f sis_tmp_task_item_processing.sql
echo "===============================SIS task_item end ========="