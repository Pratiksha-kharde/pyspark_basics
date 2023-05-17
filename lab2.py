#In mvrating.txt file assume the data as
# (1 296 5 1147880044)===(1-Movie Cataegory, 296- Total movies,
# 5- ratings, 1147..-no of users watched movie)

#1. count the total movies per category
from pyspark.sql import SparkSession
import findspark
findspark.init()

spark = SparkSession.builder.appName('SparkApp2').getOrCreate()
sc = spark.sparkContext

rdd = sc.textFile('mvratings.txt')
rdd2 = rdd.map(lambda x:(x.split()[0],1))
rdd2.reduceByKey(lambda x,y: x+y).collect()

