from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

topic_name = 'my_project_topic'

spark = SparkSession \
    .builder \
    .appName("my_project_spark_session") \
    .getOrCreate()
spark.conf.set('spark.sql.caseSensitive', True)

streaming_df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", topic_name) \
  .load()


# output_df = streaming_df.selectExpr("cast(key as string)","cast(value as string)")

# query = output_df \
#     .writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .start()
# query.awaitTermination()

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
json_expanded_df = json_df.withColumn("value", from_json(json_df["value"], json_schema)).select("value.*")
json_expanded_df = json_expanded_df.drop(json_expanded_df.columns[-1])
# json_expanded_df = json_expanded_df.drop(json_expanded_df.select(json_expanded_df.columns[-1]))
# json_expanded_df.printSchema()

query = json_expanded_df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
