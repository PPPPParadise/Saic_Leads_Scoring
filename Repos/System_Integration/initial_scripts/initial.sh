#!/bin/bash

hive -f event_tmp.sql
hive -f contact_id_tmp.sql
hive -f city_level_create.sql
hadoop fs -put city_level.txt /tmp/
hive -e "load data inpath '/tmp/city_level.txt' into table marketing_modeling.edw_city_level"