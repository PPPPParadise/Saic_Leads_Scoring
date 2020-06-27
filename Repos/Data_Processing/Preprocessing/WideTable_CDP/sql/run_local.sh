#!/bin/bash
pt=20190901
if [ `expr length $pt` == 8 ] && date -d $pt +%Y%m%d > /dev/null 2>&1; then
    echo $pt; 
else    
	echo "pt=$pt 输入的日期格式不正确，应为yyyymmdd";
	exit 1; 
fi   
echo "====================-cdp_middle_tb.sql start_1......" 
hive -hivevar pt=$pt -f cdp_middle_tb.sql >& cdp_middle_tb.log
echo "====================-cdp_mapping_tb.sql start._2....."
hive -f cdp_mapping_tb.sql >& cdp_mapping_tb.log
echo "====================-cdp_final_tb.sql start._3....."
hive -f cdp_final_tb.sql >& cdp_final_tb.log
