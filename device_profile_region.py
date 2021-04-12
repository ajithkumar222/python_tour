from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, array, concat_ws
from pyspark.sql.types import StructType, StructField, StringType,IntegerType
from pyspark.sql.functions import expr, when, first




def pose():
	spark = SparkSession.builder.appName('csql_demo1').master('local[*]').getOrCreate()
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


def asset():
	spark = SparkSession.builder.appName('csql_demo1').master('local[*]').getOrCreate()
	query = "(SELECT * FROM relation WHERE relation_type = 'Contains') as r"
	get_data = spark.read.format('jdbc').option('driver', 'org.postgresql.Driver').option('url', 'jdbc:postgresql://192.168.1.36:5432/thingsboard').option("user", "postgres").option("password", "postgres").option('dbtable', query).load()
	lat_lon = get_data.rdd.map(lambda x: [x.from_id, x.from_type, x.to_id, x.to_type]).collect()
	return lat_lon


if __name__ == "__main__":
	postgres_data = pose()
	zone_details = []
	zone_ids = asset()
	for i in zone_ids:
		if i[0] == "20dbea70-4405-11ea-8937-b56efe23a65c" and i[1] == "ASSET":
			zone_details.append(i[2])
	ward_details = []
	for j in zone_ids:
		if j[0] in zone_details:
			ward_details.append(j[2])
	device_details = []
	for k in zone_ids:
		if k[0] in ward_details:
			device_details.append(k[2])
	device_list = []
	for s in postgres_data:
		if s[1] in device_details:
			device_list.append(s)
	print device_list
	print len(device_list)