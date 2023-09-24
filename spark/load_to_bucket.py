from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F

topic_name = 'binance_data_streaming'
table_name = 'binance_data_streaming.binance_data'

def func(batch_df, batch_id):
    # batch_df.collect()
    # batch_df.printSchema()
    batch_df.write \
        .format("bigquery") \
        .mode("append") \
        .option("table", table_name) \
        .save()
    print(batch_df)

# Turn on case sensitivity for spark because request returns two identical columns 
# Include jar packages for kafka and bigquery
# Include downloaded jar file for GCS
# Provide path for my credentials
conf = SparkConf() \
	.setAppName('binance_data_streaming') \
	.set("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.15.1-beta,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
	.set("spark.jars", "/home/efischuk/binance_data_streaming/spark/lib/gcs-connector-hadoop3-2.2.5.jar") \
	.set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
	.set("temporaryGcsBucket", "efischuk_bucket") \
  	.set("spark.sql.caseSensitive", True) \
	.set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/home/efischuk/.gc/gc_key.json")

sc = SparkContext(conf=conf)

hadoop_conf = sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", "/home/efischuk/.gc/gc_key.json")
hadoop_conf.set("fs.gs.auth.service.account.enable", "true")

spark = SparkSession.builder \
	.config(conf=sc.getConf()) \
	.getOrCreate()

streaming_df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", topic_name) \
  .load()

json_schema = StructType([
	StructField('e', StringType(), True),
	StructField('E', LongType(), True),
	StructField('s', StringType(), True),
	StructField('a', LongType(), True),
	StructField('p', StringType(), True),
	StructField('q', StringType(), True),
	StructField('f', LongType(), True),
	StructField('l', LongType(), True),
	StructField('T', LongType(), True),
	StructField('m', BooleanType(), True),
	StructField('M', BooleanType(), True)
])

json_df = streaming_df.selectExpr("cast(value as string) as value")
df_binance = json_df.withColumn("value", from_json(json_df["value"], json_schema)).select("value.*")
df_binance = df_binance.drop('M')
df_binance = df_binance.withColumnRenamed('e', 'event_type').withColumnRenamed('E', 'event_time')\
	.withColumnRenamed('s', 'symbol').withColumnRenamed('a', 'agg_trade_id').withColumnRenamed('p', 'price')\
	.withColumnRenamed('q', 'quantity').withColumnRenamed('f', 'first_trade_id')\
	.withColumnRenamed('l', 'last_trade_id').withColumnRenamed('m', 'is_market_maker')\
	.withColumnRenamed('T', 'trade_time')

df_binance = df_binance.withColumn("trade_time", F.to_utc_timestamp(F.from_unixtime(F.col("trade_time")/1000,'yyyy-MM-dd HH:mm:ss'),'EST'))
df_binance = df_binance.withColumn("event_time", F.to_utc_timestamp(F.from_unixtime(F.col("event_time")/1000,'yyyy-MM-dd HH:mm:ss'),'EST'))
df_binance = df_binance.withColumn("quantity", df_binance["quantity"].cast(DecimalType(precision=12, scale=2)))
df_binance = df_binance.withColumn("price", df_binance["price"].cast(DecimalType(precision=12, scale=2)))

query = df_binance.writeStream \
  .format("bigquery") \
  .foreachBatch(func) \
  .outputMode("append") \
  .trigger(processingTime='2 minutes') \
  .option("temporaryGcsBucket","efischuk_bucket") \
  .option("checkpointLocation", "/home/efischuk/binance_data_streaming/spark/folder_for_checkpoint") \
  .option("table", table_name) \
  .start()

# query = df_binance.writeStream \
#             .format("console") \
#             .outputMode("append") \
#             .start()

query.awaitTermination()
