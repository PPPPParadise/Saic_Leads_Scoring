#!/bin/bash
queuename=`awk -F '=' '/\[HIVE\]/{a=1}a==1&&$1~/job.queuename/{print $2;exit}' ../../Data_Processing/Preprocessing/config.ini`
pt=$3
odt=`date -d $pt"+1 days" +%Y%m%d`
top=$4
echo "------------"
echo "pt=$pt"
echo "odt=$odt"
echo "top=$top"
echo "------------"
spark-submit --queue malg --master yarn --driver-memory 30G --num-executors 10 --executor-cores 8 --executor-memory 10G --conf "spark.excutor.memoryOverhead=15G" Outbound_List_Generation.py $pt $odt
hive -hivevar pt=$pt -hivevar queuename=$queuename -f SQL/Insert_Failed_Model_Result.sql
hive -hivevar pt=$pt -hivevar top=$top -hivevar queuename=$queuename -f SQL/Cust_Dealer_Info_Mapping.sql
spark-submit --queue malg --master yarn --driver-memory 30G --num-executors 10 --executor-cores 8 --executor-memory 10G --conf "spark.excutor.memoryOverhead=15G" Outbound_Info_Generation.py $pt $odt
