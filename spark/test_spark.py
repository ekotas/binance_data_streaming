from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession \
    .builder \
    .appName("my_project_spark_session") \
    .config("spark.jars", "/home/efischuk/my_project/spark/gcs-connector-latest-hadoop2.jar") \
    .getOrCreate()

my_json = '''
{
	"ordertime": 1497014222380,
	"orderid": 18,
	"itemid": "Item_184",
	"address": {
		"city": "Mountain View",
		"state": "CA",
		"zipcode": 94041
	}
}
'''

df = spark.read.format("json").load("test.json")
df.show()

df.write\
    .format("json") \
    .save("gs://efischuk_bucket/cens_cursos")