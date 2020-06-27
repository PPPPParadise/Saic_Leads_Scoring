create external table if not exists marketing_modeling.edw_city_level (
    province string,
    city_name string,
    city_level STRING
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE
