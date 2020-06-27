#!/bin/bash
pt=$3
echo "===============================userprofile.sql"
hive --hivevar pt=$pt -f userprofile.sql