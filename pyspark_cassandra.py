import json

from cassandra.cqlengine.columns import Map
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, array, concat_ws
from pyspark.sql.types import StructType, StructField, StringType,IntegerType
from pyspark.sql.functions import expr, when, first, last


def cct(tt):
	for i in tt:
		if i:
			return i


def pose():
	# SparkSession_2 = SparkSession.newSession()
	spark = SparkSession.builder.appName('csql_demo1').master('local[*]').getOrCreate()
	# spark = SparkSession.builder.appName('csql_demo').master('local[*]').config('spark.jars', 'file:///home/boopathi/Downloads/spark-cassandra-connector-2.4.0-s_2.11.jar').getOrCreate()
	# spark.conf.set('spark.jars', 'file:///home/boopathi/Downloads/postgresql-42.2.7.jar')postgresql-42.2.7.jar
	# spark.newSession() ,.config('spark.jars','file:///home/boopathi/Downloads/*')
	#--------------------
	SparkSession_2 = spark.newSession()
	# query = "(SELECT * FROM attribute_kv) as r"
	query = "(SELECT * FROM attribute_kv WHERE  entity_type = 'DEVICE' ) as r"
	get_data = spark.read.format('jdbc').option('driver', 'org.postgresql.Driver').option('url', 'jdbc:postgresql://192.168.1.36:5432/thingsboard').option("user", "postgres").option("password", "postgres").option('dbtable', query).load()
	dx = get_data.withColumn("value", concat_ws("", get_data.bool_v, get_data.long_v, get_data.dbl_v, get_data.json_v, get_data.str_v))
	dx = dx.filter(dx.attribute_type == 'SERVER_SCOPE')
	nl = dx.groupBy('entity_id', 'attribute_type').pivot('attribute_key').agg(first('value'))
	ld = nl.withColumnRenamed('entity_id', 'device_id')
	query = "(SELECT name, type, id FROM device) as r"
	sk = spark.read.format('jdbc').option('driver', 'org.postgresql.Driver').option('url', 'jdbc:postgresql://192.168.1.36:5432/thingsboard').option("user", "postgres").option("password", "postgres").option('dbtable', query).load()
	joined_data = ld.join(sk, ld.device_id == sk.id)
	req_det = joined_data.rdd.map(lambda x: [x.name, x.device_id, x.attribute_type, x.scNo, x.simNo, x.imeiNumber, x.boardNumber,
		x.zoneName, x.wardName, x.location, x.phase, x.ccmsType, x.kva, x.baseWatts, x.baseLine, x.connectedWatts,
		x.roadType, x.latitude, x.longitude]).collect()
	return req_det
	# cot = udf(lambda arr: cct(arr))
	# get_data.withColumn("value", cot(array(get_data['bool_v'].cast(StringType())))).show(10)

	# ss = get_data.withColumn('value', cot(array(get_data['bool_v'].cast(StringType()), get_data['long_v'].cast(StringType()), get_data['dbl_v'].cast(StringType()), get_data['json_v'].cast(StringType()), get_data['str_v'].cast(StringType()))))
	# tf = ss.select('entity_id', 'attribute_key','attribute_type', 'value').filter(ss.entity_type == 'DEVICE')
	# nl = tf.groupBy('entity_id', 'attribute_type').pivot('attribute_key').agg(first('value')).filter(tf.attribute_type == 'SERVER_SCOPE')
	# ld = nl.withColumnRenamed('entity_id', 'device_id')
	# ld.show(10)
	# ss.show(10)
	#-----------------
	# vv = nl.select('entity_id', 'active', 'applicationVersion', 'ieeeNumber')
	# sd = vv.groupBy('entity_id','active','applicationVersion','ieeeNumber').count()
	# get_data.toDF().map(lambda r: (r.entity_id, r.attribute_key)).reduceByKey(lambda x,y: x + y).toDF(['store','values']).show()
	# rw = []
	# n = 0
	# for i in range(0,len(fg)):
	# 	a = []
	# 	for j in range(1,len(fg)):
	# 		if fg.entity_id[i] == fg.entity_id[j]:
	# 			s = []
	# 			s.append(fg.attribute_key[j])
	# 			s.append(fg.value[j])
	# 			a.append(fg.entity_id[i])
	# query = "(SELECT * FROM device WHERE name = 'SS3225AA22014') as r"
	# get_data_1 = spark.read.format('jdbc').option('driver', 'org.postgresql.Driver').option('url', 'jdbc:postgresql://192.168.1.36:5432/thingsboard').option("user", "postgres").option("password", "postgres").option('dbtable', query).load()
	# return get_data_1
	# s = "20dbea70-4405-11ea-8937-b56efe23a65c"
	# query = "(SELECT * FROM relation where from_id = '" + str(s) + "' and to_type = 'DEVICE') as r"
	# get_data_1 = spark.read.format('jdbc').option('driver', 'org.postgresql.Driver').option('url', 'jdbc:postgresql://192.168.1.36:5432/thingsboard').option("user", "postgres").option("password", "postgres").option('dbtable', query).load()
	# return get_data_1
	# time_line = ld.join(get_data_1, ld.device_id == get_data_1.id, 'left').drop('id')
	# time_line.show(1000)
	# return time_line
	# dw = fg.join()
	# fg.show(100)
	# return nl
	# sparka = SparkSession.builder.appName('csql_demo').master('local[*]').config('spark.jars', 'file:///home/boopathi/Downloads/spark-cassandra-connector-2.4.0-s_2.11.jar').getOrCreate()
	# SparkSession.newSession(sparka)
	# SparkSession_2 = spark.newSession()1
	# 1spark.conf.set('spark.cassandra.connection.host', '192.168.1.26')
	# spark.conf.set('spark.jars', 'file:///home/boopathi/Downloads/spark-cassandra-connector-2.4.0-s_2.11.jar')
	# print SparkSession_2.conf.get('spark.jars')
	# tk = spark.read.format('org.apache.spark.sql.cassandra').options(table='ts_kv_cf', keyspace='thingsboard').load()1
	# ms = tk.select('entity_type','entity_id','key','ts')
	# ll = tk.withColumnRenamed('ts', 'time')1
	# ctt = udf(lambda arr: cct(arr))1
	# 1de = ll.withColumn('final_value', ctt(array(ll['bool_v'].cast(StringType()), ll['long_v'].cast(StringType()),ll['dbl_v'].cast(StringType()),ll['json_v'].cast(StringType()),ll['str_v'].cast(StringType()))))
	# 1dr = de.groupBy('entity_id','entity_type','time').pivot('key').agg(first('final_value'))
	# 1nanu = dr.filter((dr.time).between(1610926262757,1610929862758))
	# 1sss = nanu.filter(dr.entity_id == '7214d110-5181-11eb-a9d5-3fced82b8b0f')
	# 1sss.show()


def cass_n():
	spark = SparkSession.builder.appName('csql_demo2').master('local[*]').getOrCreate()
	# .config('spark.jars', 'file:///home/boopathi/Downloads/spark-cassandra-connector-2.4.0-s_2.11.jar')
	# spark.conf.set('spark.cassandra.connection.host', '192.168.1.36')
	# query = "SELECT * FROM ts_kv_cf"
	# tk = spark.read.format('org.apache.spark.sql.cassandra').options(table='ts_kv_cf', keyspace='thingsboard').load()
	tk = spark.read.format("org.apache.spark.sql.cassandra").option("spark.cassandra.connection.host", "192.168.1.36").options(table='ts_kv_cf', keyspace='thingsboard').load()
	s = tk.filter((tk.entity_id == '6431fd80-48cf-11eb-aa37-1fc06eba198d') & ((tk.ts).between('1617733800000', '1617820200000')))
	# sk = s.withColumn("value", concat_ws("", s.bool_v, s.long_v, s.dbl_v, s.json_v, s.str_v))
	s.show(1000000)

	# tk.createOrReplaceTempView("usertable")
	# dataset1 = spark.sql("select * from usertable where entity_id == '6431fd80-48cf-11eb-aa37-1fc06eba198d' and ts between '1617733800000' and '1617820200000' ")
	# dataset1.show(10000)

# sd = sk.select('entity_type', 'entity_id', 'key', 'ts', 'value')
	# results = sk.toJSON().map(lambda j: json.loads(j)).collect()
	# for i in results:
	# 	print i['entity_id']
	# req_det = sk.rdd.map(lambda x: [x.entity_type, x.entity_id, x.key, x.value, x.ts]).collect()
	# print req_det
	# piv_value = sd.groupBy('entity_id', 'entity_type', 'ts').pivot('key').agg(first('value'))
	# piv_value.show()
	# req_det = tk.rdd.map(lambda x: [x.entity_type, x.entity_id, x.key, x.ts]).collect()
	# ll = tk.filter((tk.ts).between('1617733800000', '1617820200000'))
	# sk = ll.withColumn("value", concat_ws("", ll.bool_v, ll.long_v, ll.dbl_v, ll.json_v, ll.str_v))
	# ms = sk.select('entity_type', 'entity_id', 'key', 'ts', 'value')
	# req_det = sk.rdd.map(lambda x: [x.entity_type, x.entity_id, x.key, x.value, x.ts]).collect()
	# print req_det
	# return req_det
	# pj = ms.groupBy('entity_id', 'entity_type', 'ts').count()
	# pj.show()
	# piv_value = tk.groupBy('entity_id', 'entity_type', 'ts').pivot('key').agg(first('long_v'))
	# piv_value.show(10)

	# ctt = udf(lambda arr: cct(arr))
	# de = ll.withColumn('final_value', ctt(array(ll['bool_v'].cast(StringType()), ll['long_v'].cast(StringType()), ll['dbl_v'].cast(StringType()), ll['json_v'].cast(StringType()), ll['str_v'].cast(StringType()))))
	# dr = de.groupBy('entity_id', 'entity_type', 'time').pivot('key').agg(first('final_value'))
	# dr.select('active')
	# nanu = dr.filter((dr.time).between(1610926262757,1610929862758))
	# sss = nanu.filter(dr.entity_id == '7214d110-5181-11eb-a9d5-3fced82b8b0f')
	# sss.show(10)
	# return dr


def asset(s=None):
	spark = SparkSession.builder.appName('csql_demo1').master('local[*]').getOrCreate()
	query = "(SELECT * FROM relation WHERE relation_type = 'Contains') as r"
	get_data = spark.read.format('jdbc').option('driver', 'org.postgresql.Driver').option('url', 'jdbc:postgresql://192.168.1.36:5432/thingsboard').option("user", "postgres").option("password", "postgres").option('dbtable', query).load()
	# get_data = get_data.filter(get_data.from_id == str(s))
	# get_data.show(10)
	lat_lon = get_data.rdd.map(lambda x: [x.from_id, x.from_type, x.to_id, x.to_type]).collect()
	return lat_lon
	# get_data.show(10)


def asset_name(s=None):
	spark = SparkSession.builder.appName('csql_demo1').master('local[*]').getOrCreate()
	query = "(SELECT id FROM asset WHERE name  = '" + s + "') as r"
	get_data = spark.read.format('jdbc').option('driver', 'org.postgresql.Driver').option('url', 'jdbc:postgresql://192.168.1.36:5432/thingsboard').option("user", "postgres").option("password", "postgres").option('dbtable', query).load()
	# get_data = get_data.filter(get_data.from_id == str(s))
	lat_lon = get_data.rdd.map(lambda x: [x.id]).collect()
	return lat_lon


def device_name():
	spark = SparkSession.builder.appName('csql_demo1').master('local[*]').getOrCreate()
	query = "(SELECT name, type, id FROM device) as r"
	get_data = spark.read.format('jdbc').option('driver', 'org.postgresql.Driver').option('url', 'jdbc:postgresql://192.168.1.36:5432/thingsboard').option("user", "postgres").option("password", "postgres").option('dbtable', query).load()
	# get_data = get_data.filter(get_data.from_id == str(s))
	# get_data.show()
	# lat_lon = get_data.rdd.map(lambda x: [x.id]).collect()
	return get_data


def dev(k=None):
	spark = SparkSession.builder.appName('csql_demo1').master('local[*]').getOrCreate()
	query = "(SELECT from_id,from_type FROM relation where to_id = '" + str(k) + "' ) as r"
	get_data = spark.read.format('jdbc').option('driver', 'org.postgresql.Driver').option('url', 'jdbc:postgresql://192.168.1.26:5432/thingsboard').option("user", "postgres").option("password", "schnell").option('dbtable', query).load()
	# lat_lon = get_data.rdd.map(lambda x : [x.from_id, x.from_type]).collect()
	# return lat_lon
	# return get_data.rdd.map(lambda x: [x.id]).collect()
	# get_data.show()

def convert_dataframe(l=None):
	spark = SparkSession.builder.appName('conversion').master('local[*]').getOrCreate()
	k = spark.createDataFrame(l, ['r_id', 'r_type'])
	return k


if __name__ == "__main__":
	cassandra_data = cass_n()
	print cassandra_data
	# cassandra_data.show(10)
	# print "checking the pyspark database data "
	# postgres_data = pose()
	# print postgres_data
	# postgres_data.show(1000)
	# joined_data = cassandra_data.join(postgres_data, cassandra_data.entity_id == postgres_data.device_id, 'right')
	# joined_data.show(100)
	# k = asset_name('CJB')
	# print k
	# device_name()
	#MAA- 8edc23e0-ca8e-11ea-84f8-9d14b04f9b88
	#CJB - 20dbea70-4405-11ea-8937-b56efe23a65c
	# zone_details = []
	# zone_ids = asset('8edc23e0-ca8e-11ea-84f8-9d14b04f9b88')
	# for i in zone_ids:
	# 	if i[0] == "8edc23e0-ca8e-11ea-84f8-9d14b04f9b88" and i[1] == "ASSET":
	# 		zone_details.append(i[2])
	# # print zone_details
	# ward_details = []
	# for j in zone_ids:
	# 	if j[0] in zone_details:
	# 		ward_details.append(j[2])
	# # print ward_details
	# device_details = []
	# for k in zone_ids:
	# 	if k[0] in ward_details:
	# 		device_details.append(k[2])
	# device_list = []
	# for s in postgres_data:
	# 	if s[1] in device_details:
	# 		device_list.append(s)
	#
	# print device_list
	# print len(device_list)



	# ward_ids = asset(zone_ids[0][0])
	# device_ids = asset(ward_ids[0][0])
	# print zone_ids[0][0]
	# print ward_ids
	# k = dev(1)
	# print(k)
	# print(asset(k[0][0]))
	# print device_ids
	# d = convert_dataframe(device_ids)
	# d.show()
	# relative_join = joined_data.join(d, joined_data.device_id == d.r_id, 'inner')
	# print relative_join.collect()
	# cass_n()
	# spark = SparkSession.builder.appName('csql_demo').getOrCreate()
	# .master('local[*]').config('spark.jars', 'file:///home/boopathi/Downloads/spark-cassandra-connector-2.4.0-s_2.11.jar')
		# .config('spark.executor.extraClassPath', 'file:///home/boopathi/Desktop/jar_files_2/spark-cassandra-connector_2.10-1.6.0-M1.jar')\
		# .config('spark.executor.extraLibrary', 'file:///home/boopathi/Desktop/jar_files_2/spark-cassandra-connector_2.10-1.6.0-M1.jar')\
		# .config('spark.driver.extraClassPath', 'file:///home/boopathi/Desktop/jar_files_2/spark-cassandra-connector_2.10-1.6.0-M1.jar')\
		# .enableHiveSupport().getOrCreate()
	# spark = SparkSession.builder.appName('demo_cassa').getOrCreate()
	# spark.conf.set('spark.jars', 'file:///home/boopathi/Downloads/spark-cassandra-connector-2.4.0-s_2.11.jar')
	# spark.conf.set("spark.cassandra.connection.host", "127.0.0.1")
	# get_data = spark.read.format('jdbc').option('driver','org.apache.spark.sql.cassandra').option('url','jdbc:cassandra://127.0.0.1:9042/spark_testing').option('dbtable', 'emp').load()
	# get_data = spark.read.format("org.apache.spark.sql.cassandra").options(table="emp", keyspace="spark_testing").load()
	# get_data.show()
	# .config('spark.jars', 'file:///home/boopathi/Desktop/jar_files_2/spark-cassandra-connector_2.10-1.6.0-M1.jar')