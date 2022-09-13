Databricks notebook source

from pyspark.sql import SparkSession

spark = SparkSession.builder.master("yarn").appName("expedia").getOrCreate()





# COMMAND ----------




dbutils.fs.unmount("/mnt/expedia")




# COMMAND ----------




# DBTITLE 1,Mount storagecontainer

# MAGIC %python

# MAGIC dbutils.fs.mount(

# MAGIC source = "wasbs://expedia@storage2341.blob.core.windows.net",

# MAGIC mount_point = "/mnt/expedia",

# MAGIC extra_configs = {"fs.azure.account.key.storage2341.blob.core.windows.net": "Rg7Dt6Kf3UxWvlRJ48H+fu9g2B2auwdPziJTZbvvJJvVuQhD8XvMWZGnkR/reJyJZTSFu8LZ8nY1TfwyXgHS+A=="})




# COMMAND ----------




# DBTITLE 1,Read data from blobstorage

from pyspark.sql.types import *






df=spark.read.format("avro").option("inferSchema","True").load("/mnt/expedia/*.avro")

df.count()




# COMMAND ----------




# MAGIC %python

# MAGIC jdbcHostname = "sqlserverepamanuragpoc.database.windows.net"

# MAGIC jdbcDatabase = "sqlserver"

# MAGIC jdbcusername = "adminepam"

# MAGIC jdbcpassword = "Anurag_0301"

# MAGIC jdbcport = "1433"

# MAGIC table = "expedia"

# MAGIC jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcport, jdbcDatabase)




# COMMAND ----------




df.write.mode("overwrite")\

.format("jdbc")\

.option("url",jdbcUrl)\

.option("dbtable",table)\

.option("user",jdbcusername)\

.option("password",jdbcpassword)\

.option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")\

.save()




[2/7 3:35 PM] Nagesh Mungikar


dbutils.fs.unmount("/mnt/expedia")





# COMMAND ----------




# DBTITLE 1,Mount storagecontainer

# MAGIC %python

# MAGIC dbutils.fs.mount(

# MAGIC source = "wasbs://expedia@storage2341.blob.core.windows.net",

# MAGIC mount_point = "/mnt/expedia",

# MAGIC extra_configs = {"fs.azure.account.key.storage2341.blob.core.windows.net": "Rg7Dt6Kf3UxWvlRJ48H+fu9g2B2auwdPziJTZbvvJJvVuQhD8XvMWZGnkR/reJyJZTSFu8LZ8nY1TfwyXgHS+A=="})




[2/7 3:42 PM] Nagesh Mungikar


# Databricks notebook source

from pyspark.sql import SparkSession

spark = SparkSession.builder.master("yarn").appName("expedia").getOrCreate()





# COMMAND ----------




dbutils.fs.unmount("/mnt/expedia")




# COMMAND ----------




# DBTITLE 1,Mount storagecontainer

# MAGIC %python

# MAGIC dbutils.fs.mount(

# MAGIC source = "wasbs://expedia@storage2341.blob.core.windows.net",

# MAGIC mount_point = "/mnt/expedia",

# MAGIC extra_configs = {"fs.azure.account.key.storage2341.blob.core.windows.net": "Rg7Dt6Kf3UxWvlRJ48H+fu9g2B2auwdPziJTZbvvJJvVuQhD8XvMWZGnkR/reJyJZTSFu8LZ8nY1TfwyXgHS+A=="})




# COMMAND ----------




# DBTITLE 1,Read data from blobstorage

from pyspark.sql.types import *






df=spark.read.format("avro").option("inferSchema","True").load("/mnt/expedia/*.avro")

df.count()




# COMMAND ----------




# MAGIC %python

# MAGIC jdbcHostname = "sqlserverepamanuragpoc.database.windows.net"

# MAGIC jdbcDatabase = "sqlserver"

# MAGIC jdbcusername = "adminepam"

# MAGIC jdbcpassword = "Anurag_0301"

# MAGIC jdbcport = "1433"

# MAGIC table = "expedia"

# MAGIC jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcport, jdbcDatabase)




# COMMAND ----------




df.write.mode("overwrite")\

.format("jdbc")\

.option("url",jdbcUrl)\

.option("dbtable",table)\

.option("user",jdbcusername)\

.option("password",jdbcpassword)\

.option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")\

.save()




# COMMAND ----------




# DBTITLE 1,Delta for expedia

# MAGIC %sql

# MAGIC drop table conf.runtimeinfo;

# MAGIC create table conf.runtimeinfo(

# MAGIC tablename string,

# MAGIC start_time timestamp)

# MAGIC USING DELTA;




# COMMAND ----------




# MAGIC %sql

# MAGIC insert into table conf.runtimeinfo values('expedia',cast('1900-01-01' as date))




# COMMAND ----------




# DBTITLE 1,Convert to datetime format

import time

ts = time.time()

import datetime

ts1=spark.sql("select start_time from conf.runtimeinfo where tablename='expedia'").collect()[0][0].strftime('%Y-%m-%d %H:%M:%S')

query ="( select * from dbo.expedia where date_time>=cast('{0}' as datetime) ) z".format(ts1)

today1=datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

today1




# COMMAND ----------






jdbcHostname = "sqlserverepamanuragpoc.database.windows.net"

jdbcDatabase = "sqlserver"

jdbcusername = "adminepam"

jdbcpassword = "Anurag_0301"

jdbcport = "1433"

table = "expedia"

jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcport, jdbcDatabase)

query1="""update conf.runtimeinfo

set start_time= cast('{0}' as timestamp)

where tablename='expedia'""".format(today1)

delta=spark.read \

.format("jdbc")\

.option("url",jdbcUrl)\

.option("dbtable",query)\

.option("user",jdbcusername)\

.option("password",jdbcpassword)\

.option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver").option("numPartitions",20).option("partitionColumn","id").option("lowerBound",1).option("upperBound",2528239).load()

delta.count()




# COMMAND ----------




final=spark.read.format('jdbc').options(url='jdbc:sqlserver://anurag.sql.azuresynapse.net:1433;database=pool2',dbtable='final_expedia',user='sqladminuser@anurag',password='Anurag_0301').load().limit(5)

final.count()




# COMMAND ----------




delta.registerTempTable("delta")

final.registerTempTable("final")




# COMMAND ----------




# DBTITLE 1,Incremental load for expedia data

target=spark.sql("""

select * from delta

union all

select f.* from final f left anti join delta d on d.id=f.id and d.hotel_id=f.hotel_id

""").limit(20000)

target.count()




# COMMAND ----------




# DBTITLE 1,Writing expedia data to synapse table

target.write.mode("overwrite").option("truncate",True).format('jdbc').options(url='jdbc:sqlserver://anurag.sql.azuresynapse.net:1433;database=pool2',dbtable='final_expedia',user='sqladminuser@anurag',password='Anurag_0301').save()




# COMMAND ----------




spark.sql(query1)




# COMMAND ----------




spark.sql("select * from conf.runtimeinfo").show()




# COMMAND ----------




# DBTITLE 1,Mount hotel storage location

dbutils.fs.mount(

source = "wasbs://hotel@storage2341.blob.core.windows.net",

mount_point = "/mnt/hotel",

extra_configs = {"fs.azure.account.key.storage2341.blob.core.windows.net": "Rg7Dt6Kf3UxWvlRJ48H+fu9g2B2auwdPziJTZbvvJJvVuQhD8XvMWZGnkR/reJyJZTSFu8LZ8nY1TfwyXgHS+A=="})




# COMMAND ----------




file_location='/mnt/hotel/*.csv'

hotel_raw=spark.read.csv(file_location,inferSchema=True,header=True)

hotel_raw.createOrReplaceTempView("Hotel_raw")

cn=spark.sql(" select count(*) as Count from Hotel_raw ")

cn.show()




# COMMAND ----------




# DBTITLE 1,Invalid hotel records

hotel_raw.createOrReplaceTempView("invalid")

hotel_invalid=spark.sql("select * from invalid where latitude is null or longitude is null or latitude rlike 'NA' or longitude rlike 'NA'")




# COMMAND ----------




pip install pygeohash




# COMMAND ----------




pip install opencage




# COMMAND ----------




# DBTITLE 1,Functions for geohash

from opencage.geocoder import OpenCageGeocode

import pygeohash as geohash




key = '58062003d54b49eb8bcd74b6355f09e4'

geocoder=OpenCageGeocode(key)




def get_lat_long(name, address, city, country, tag):

value=geocoder.geocode('{},{},{},{}'.format(name,address,city,country))

return (value[0]['geometry'] [tag])

udf_lat_long=udf(get_lat_long)






def geohashfunc(lat,long):

if lat is None or long is None:

lat=0.0

long=0.0

return(geohash.encode(lat,long,precision=5))

geohash_udf=udf(geohashfunc)




# COMMAND ----------




from pyspark.sql.functions import col,lit

hotel_mod = hotel_invalid.withColumn("g.lattitude",udf_lat_long(col("name"),col("address"),col("city"),col("country"),lit("lat"))).withColumn("g.longittude",udf_lat_long(col("name"),col("address"),col("city"),col("country"),lit("lng"))).drop('latitude','Longitude')

display(hotel_mod)






# COMMAND ----------




hotel_valid=hotel_raw.subtract(hotel_invalid)

hotel_final=hotel_valid.union(hotel_mod)

hotel_gfinal=hotel_final.withColumn("geohash",geohash_udf(col("latitude").cast("float"),col("longitude").cast("float")))




# COMMAND ----------




# DBTITLE 1,Write hotel data to synapse db

hotel_gfinal.write.mode("overwrite").option("truncate",True).format('jdbc').options(url='jdbc:sqlserver://anurag.sql.azuresynapse.net:1433;database=pool2',dbtable='hotel',user='sqladminuser@anurag',password='Anurag_0301').save()




# COMMAND ----------




pip install azure-functions




# COMMAND ----------




blob_container = 'weatherraw'

storage_account_name = 'storage2341'

filePath = "wasbs://" + blob_container + "@" + storage_account_name + ".blob.core.windows.net/weather/"

df = spark.read.format("parquet").load(filePath)

df.show(100,False)




# COMMAND ----------




from pyspark.sql.functions import *

df = df.withColumn('key', to_json(struct([df[col] for col in df.columns]))).select('key')

df.show(10,False)




# COMMAND ----------




df1 = df.withColumnRenamed('key', 'body')




# COMMAND ----------




connection_string="Endpoint=sb://eventhubanurag.servicebus.windows.net/;SharedAccessKeyName=policyname;SharedAccessKey=Qk2BgjFHi0eq2Cl2ZLxyNUOgeRDGV9oyznzJ11n8Jwg=;EntityPath=evenhub"

eventhub_conf = {}

eventhub_conf['eventhubs.connectionString'] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connection_string)




# COMMAND ----------




# DBTITLE 1,Write data to eventhub

df2 = df1

df2.write.format('eventhubs').options(**eventhub_conf).save()




# COMMAND ----------




# DBTITLE 1,Read data from eventhub

import json

start_event_position ={

"offset": "-1",

"seqNo": -1,

"enqueuedTime": None,

"isInclusive": True

}




eh_conf = {}

eh_conf["eventhubs.connectionString"] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connection_string)

eh_conf["eventhubs.startingPosition"] = json.dumps(start_event_position)

weather_df = spark.read.format("eventhubs").options(**eh_conf).load()






# COMMAND ----------




from pyspark.sql.types import *

weather_schema = StructType([

StructField("lng", DoubleType()),

StructField("lat", DoubleType()),

StructField("avg_tmpr_f", DoubleType()),

StructField("avg_tmpr_c", DoubleType()),

StructField("wthr_date", StringType()),

StructField("year", IntegerType()),

StructField("month", IntegerType()),

StructField("day", IntegerType()),

])

weather_df1 = weather_df.selectExpr("cast (body as string) as json").select(from_json("json", weather_schema).alias("data")).select("data.*")

weather_df1.show(10,False)




# COMMAND ----------




spark.conf.set("fs.azure.account.key.storage2341.blob.core.windows.net","Rg7Dt6Kf3UxWvlRJ48H+fu9g2B2auwdPziJTZbvvJJvVuQhD8XvMWZGnkR/reJyJZTSFu8LZ8nY1TfwyXgHS+A==")




# COMMAND ----------




weather_df2=weather_df1.withColumn("geohash_weather",geohash_udf(col("lat").cast("float"),col("lng").cast("float")))

weather_df2.show(10,False)




# COMMAND ----------




# DBTITLE 1,Write weather data to synapse db

weather_df2.limit(10000).write.format('jdbc').options(url='jdbc:sqlserver://anurag.sql.azuresynapse.net:1433;database=pool2',dbtable='weather',user='sqladminuser@anurag',password='Anurag_0301').mode('append').save()




# COMMAND ----------




synapse_url = "jdbc:sqlserver://anurag.sql.azuresynapse.net:1433;database=pool2;user=sqladminuser@anurag;password=Anurag_0301;"

expedia_table = "dbo.final_expedia"

hotel_table = 'dbo.hotel'

weather_table = 'dbo.weather'




# COMMAND ----------




expedia_df = spark.read.format('jdbc').options(url=synapse_url, dbtable = expedia_table).load()

expedia_df.show(10,False)




# COMMAND ----------




hotel_df = spark.read.format('jdbc').options(url=synapse_url, dbtable = hotel_table).load()

hotel_df.show(10,False)




# COMMAND ----------




weather_df = spark.read.format('jdbc').options(url=synapse_url, dbtable = weather_table).load()

weather_df.show(10,False)




# COMMAND ----------




condition = [(hotel_df['geohash'] == weather_df['geohash_weather']) \

| (hotel_df['geohash'][1:4] == weather_df['geohash_weather'][1:4]) \

| (hotel_df['geohash'][1:3] == weather_df['geohash_weather'][1:3])]




# COMMAND ----------




hotel_weather_precision_df = hotel_df.join(weather_df, condition, "inner")

hotel_weather_precision_df.show(10,False)




# COMMAND ----------




hotel_weather_precision_df = hotel_weather_precision_df.withColumn('precision', \

when(col('geohash') == col('geohash_weather'), lit(5)) \

.when(col('geohash')[1:4] == col('geohash_weather')[1:4], lit(4)) \

.when(col('geohash')[1:3] == col('geohash_weather')[1:3], lit(3)))

hotel_weather_precision_df.show(10,False)




# COMMAND ----------




# DBTITLE 1,Idle days b/w bookings

from pyspark.sql import Window

expedia_idle_days = expedia_df.withColumn('idle_days', datediff(col('srch_ci'), lag(col('srch_ci'),1).over(Window.partitionBy(col('hotel_id')).orderBy(col('srch_ci')))))

expedia_idle_days = expedia_idle_days.fillna(0,("idle_days"))

expedia_idle_days.show(10,False)




# COMMAND ----------




# DBTITLE 1,Valid Bookings

valid_bookings = expedia_idle_days.filter(((col('idle_days') >=2) & (col('idle_days') <=30)))

valid_bookings.show(10,False)




# COMMAND ----------




# DBTITLE 1,Save invalid bookings in publish db

invalid_bookings = expedia_idle_days.subtract(valid_bookings)

invalid_bookings.show(10,False)

invalid_bookings.coalesce(2).write.format('orc').mode('append').saveAsTable('publish_db.invalid_hotels')




# COMMAND ----------




expedia_hotel_df = valid_bookings.join(hotel_df, valid_bookings['hotel_id'] == hotel_df['id'], 'left')

expedia_hotel_df.show(10,False)




# COMMAND ----------




bookings_by_country = expedia_hotel_df.groupBy('country').count()

bookings_by_country.show()




# COMMAND ----------




bookings_by_city = expedia_hotel_df.groupBy('city').count()

bookings_by_city.show()




# COMMAND ----------




# DBTITLE 1,Saving data in publish db

bookings_by_country.coalesce(1).write.format('orc').mode('append').saveAsTable('publish_db.bookings_by_country')

bookings_by_city.coalesce(1).write.format('orc').mode('append').saveAsTable('publish_db.bookings_by_city')

valid_bookings_insert = valid_bookings.withColumn('year_srch_ci', year(to_date('srch_ci')))

valid_bookings_insert.coalesce(2).write.format('orc').mode('append').partitionBy('year_srch_ci').saveAsTable('publish_db.valid_hotels')




# COMMAND ----------




valid_bookings = valid_bookings.withColumnRenamed('id', 'booking_id')

combine_df = valid_bookings.join(hotel_weather_precision_df, valid_bookings['hotel_id'] == hotel_weather_precision_df['id'], 'left')

combine_df.show(10,False)




# COMMAND ----------




combine_avg_temp = combine_df.filter(col('avg_tmpr_c') > 0.0)

combine_avg_temp.show(10,False)




# COMMAND ----------




# DBTITLE 1,Stay Duration

stay_duration_df = combine_avg_temp.withColumn('stay_duration', datediff(col('srch_co'), col('srch_ci')))

stay_duration_df.show(10,False)

stay_duration_df.printSchema()




# COMMAND ----------




preferred_stay_df = stay_duration_df.withColumn('cust_preference', when(((col('stay_duration').isNull()) | (col('stay_duration') <= 0) | (col('stay_duration') >= 30)), lit("Erroneous data")).when(col('stay_duration') == 1, lit('Short Stay'))

.when(((col('stay_duration') >= 1) | (col('stay_duration') <= 7)), lit('Standard Stay'))

.when(((col('stay_duration') >= 8) | (col('stay_duration') <=14)), lit('Standard Extended Stay'))

.when(((col('stay_duration') >14) | (col('stay_duration') <=28)), lit('Long Stay')))


preferred_stay_df.show(10,False)




# COMMAND ----------




preferred_stay_df1 = preferred_stay_df.groupBy("hotel_id", "cust_preference").agg(count("hotel_id").alias("Hotel_Max_Count")).sort(desc("Hotel_Max_Count")).limit(1)

preferred_stay_df1.show(10,False)




# COMMAND ----------




# DBTITLE 1,Children presence in stay

final_df = stay_duration_df.withColumn('with_children', when(col('srch_children_cnt') > 0, lit("Yes")).otherwise(lit("No")))

final_df.show(10,False)

final_df.coalesce(2).write.format('orc').saveAsTable('publish_db.child_presence')