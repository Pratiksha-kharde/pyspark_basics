from pyspark.sql.types import StructType, IntegerType, StringType, StructField
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkApp2').getOrCreate()
sc = spark.sparkContext

prod_data = spark.read.load('product.txt')
prod_data.show()
schema = StructType([
    StructField("ProdID", IntegerType(), True),
    StructField("ProdName", StringType(), True),
    StructField("Cost", IntegerType(), True),
    StructField("Color", StringType(), True)
])

df = spark.createDataFrame(data=prod_data, schema=schema)
df.show()
df.filter((df.ProdName == "RED") & (df.Cost >= 5000)).show()
