#!/bin/bash
pt=20190901
echo "---------------------cust_vehicle_feature_cleansing.sql start_1......" 
hive -hivevar pt=$3 -f cust_vehicle_feature_cleansing.sql