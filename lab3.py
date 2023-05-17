from pyspark.sql.functions import max, col
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkApp2').getOrCreate()
sc = spark.sparkContext

friend_df = spark.read.csv('fakefriends.csv', header=True, inferSchema=True)
friend_df.createOrReplaceTempView('test')
df1 = spark.sql("SELECT *  FROM test WHERE Name LIKE 'A%' OR Name LIKE 'W%' OR Name LIKE 'G%' OR Name LIKE 'K%' ")
df_hund = df1.groupBy("Name").agg(max("friends").alias("friends")).where(col("friends") >= 100)
df_hund.show()
