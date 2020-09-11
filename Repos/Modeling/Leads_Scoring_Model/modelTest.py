# coding:utf-8

from pyspark import SparkContext
from pyspark.sql import SparkSession,HiveContext,Window
from pyspark.sql import functions as fn
from pyspark.sql.types import IntegerType, FloatType, DoubleType, ArrayType, StringType, DecimalType,MapType

spark_session = SparkSession.builder.enableHiveSupport().appName("test").config("spark.driver.memory","30g").getOrCreate()
hc = HiveContext(spark_session.sparkContext)
# hc.setConf("hive.exec.dynamic.partition.mode", "nonstrict")


result = hc.createDataFrame(
[	("15764234561",1,None),
	("15764234521",1,None),
	("15767234561",1,None),
	("15164234561",1,None),
	("15764224561",1,None)
	],
['mobile','score','timest']
)

result.show(10)
result = result.createOrReplaceTempView("tmp_result")
insertsql= """
	insert overwrite table  marketing_modeling.app_model_result partition (pt='22222222')
	select * from tmp_result
"""
hc.sql(insertsql)