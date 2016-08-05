import os
from pyspark import SparkContext, SparkConf, HiveContext
from pyspark.sql import SQLContext
os.environ['PYSPARK_PYTHON'] = '/opt/anaconda2-4.0.0/bin/python'
conf = SparkConf()
conf.set('spark.master', 'yarn-client')
conf.set('spark.app.name', 'pipeline_test_andy')
conf.set('spark.yarn.queue', 'default')
conf.set('spark.executor.instances', '8')
conf.set('spark.executor.memory', '10g')
conf.set('spark.executor.cores', '5')
conf.set('spark.driver.memory', '10g')
conf.set('spark.driver.maxResultSize', '15g')
# conf.set('spark.dynamicAllocation.enabled', 'True')
conf.set('spark.jars', 'hdfs://ha/user/mlb/tmp/tony/sqljdbc4.jar')

sc = SparkContext(conf=conf)

sqlContext = SQLContext(sc)
hc = HiveContext(sc)


import
