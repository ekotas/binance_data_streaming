from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import *
from pyspark.sql.types import *

class SparkInstance:
    def __init__(self):
        self.spark = None
        self.schema = StructType([
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

        self.conf = SparkConf() \
            .setAppName('binance_data_streaming') \
            .set("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.15.1-beta,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
            .set("spark.jars", "/home/efischuk/binance_data_streaming/spark/lib/gcs-connector-hadoop3-2.2.5.jar") \
            .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
            .set("temporaryGcsBucket", "efischuk_bucket") \
            .set("spark.sql.caseSensitive", True) \
            .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/home/efischuk/.gc/gc_key.json")

        sc = SparkContext(conf=self.conf)

        hadoop_conf = sc._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", "/home/efischuk/.gc/gc_key.json")
        hadoop_conf.set("fs.gs.auth.service.account.enable", "true")
            
    def start_spark_session(self, config=None):
        try:
            self.spark = SparkSession.builder.config(conf=self.conf).getOrCreate()
            print('Successfully created spark session')
        except AnalysisException as e:
            print(f"Caught a SparkException:{e}, couldn`t create Spark session")
            sys.exit(1)
		
    def stop_spark_session(self):
        if self.spark is not None:
            self.spark.stop()

    def clean_timestamp(self, df, column_name):
        return df.withColumn(column_name, to_utc_timestamp(from_unixtime(col(column_name)/1000\
                                                                              ,'yyyy-MM-dd HH:mm:ss'),'EST'))
    
    def cast_string_to_float(self, df, column_name):
        return df.withColumn(column_name, df[column_name].cast(DecimalType(precision=12, scale=2)))
    
    def get_df_from_json(self, json):
        json_df = json.selectExpr("cast(value as string) as value")
        df = json_df.withColumn("value", from_json(json_df["value"], self.schema)).select("value.*")
        df = df.drop('M')
        df = df.withColumnRenamed('e', 'event_type').withColumnRenamed('E', 'event_time')\
            .withColumnRenamed('s', 'symbol').withColumnRenamed('a', 'agg_trade_id').withColumnRenamed('p', 'price')\
            .withColumnRenamed('q', 'quantity').withColumnRenamed('f', 'first_trade_id')\
            .withColumnRenamed('l', 'last_trade_id').withColumnRenamed('m', 'is_market_maker')\
            .withColumnRenamed('T', 'trade_time')
        
        return df
    
    def read_kafka_df(self, topic_name):
        self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", topic_name) \
            .load()

    def write_df_by_batch(self, df, func, destination='console', table_name=''):
        try:
            if destination == 'console':
                query = df.writeStream \
                    .format("console") \
                    .outputMode("append") \
                    .start()
            elif destination == 'bigquery':
                query = df.writeStream \
                    .format("bigquery") \
                    .foreachBatch(func) \
                    .outputMode("append") \
                    .option("temporaryGcsBucket","efischuk_bucket") \
                    .option("checkpointLocation", "/home/efischuk/binance_data_streaming/spark/folder_for_checkpoint") \
                    .option("table", table_name) \
                    .start()
            else:
                raise AnalysisException
        except AnalysisException as e:
            print(f'Couldn`t write data, please check your inputs')
            self.stop_spark_session()
            sys.exit(1)
        query.awaitTermination()