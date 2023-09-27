from pyspark.sql.functions import *
from classes.spark_instance import SparkInstance
from pyspark.sql.types import *

spark_instance = SparkInstance()

topic_name = 'binance_data_streaming'
table_name = 'binance_data_streaming.binance_data'

def push_batch_to_bigquery(batch_df, batch_id):
    batch_df.write \
        .format("bigquery") \
        .mode("append") \
        .option("table", table_name) \
        .save()
    print(f"Pushed batch #{batch_id}")

# Start spark session
spark_instance.start_spark_session()

streaming_df = spark_instance.spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", topic_name) \
  .load()

# Create and clean dataframe
df_binance = spark_instance.get_df_from_json(streaming_df)
df_binance = spark_instance.clean_timestamp(df_binance, 'trade_time')
df_binance = spark_instance.clean_timestamp(df_binance, 'event_time')
df_binance = spark_instance.cast_string_to_float(df_binance, 'quantity')
df_binance = spark_instance.cast_string_to_float(df_binance, 'price')

spark_instance.write_df_by_batch(df_binance, push_batch_to_bigquery, 'bigquery', table_name)
