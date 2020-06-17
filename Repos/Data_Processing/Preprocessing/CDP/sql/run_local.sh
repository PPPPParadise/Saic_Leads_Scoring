#!/bin/bash
pt=20190901
echo "---------------------cdp_middle_tb.sql start_1......" 
hive -hivevar pt=$pt -f cdp_middle_tb.sql >& cdp_middle_tb.log
echo "---------------------cdp_middle_tb.sql end ......"
echo "---------------------cdp_mapping_tb.sql start._2....."
hive -f cdp_mapping_tb.sql >& cdp_mapping_tb.log
echo "---------------------cdp_mapping_tb.sql end ......"
echo "---------------------cdp_final_tb.sql start._2....."
hive -f cdp_final_tb.sql >& cdp_final_tb.log
echo "---------------------cdp_final_tb.sql end ......"
