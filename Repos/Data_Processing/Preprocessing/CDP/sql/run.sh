#!/bin/bash
pt=20190901
echo "---------------------cdp_middle_tb.sql start_1......" 
hive -hivevar pt=$3 -f cdp_middle_tb.sql
echo "---------------------cdp_mapping_tb.sql start._2....."
hive -f cdp_mapping_tb.sql
echo "---------------------cdp_final_tb.sql start._2....."
hive -f cdp_final_tb.sql