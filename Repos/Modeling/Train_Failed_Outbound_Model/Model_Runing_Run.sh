#!/bin/bash
queuename=`awk -F '=' '/\[HIVE\]/{a=1}a==1&&$1~/job.queuename/{print $2;exit}' ../../Data_Processing/Preprocessing/config.ini`
pt=$3

python Model_Running.py $pt
