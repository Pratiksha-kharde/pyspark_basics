from pyspark.sql import SparkSession
import findspark
findspark.init()

spark = SparkSession.builder.appName('SparkApp2').getOrCreate()

data = [["Mark", "Male", "BCA"], ["Sahil", "Male", "BBA"],
        ["Ron", "Male", "BA"], ["Sam", "Male", "MCA"], ["Ron", "Female", "BCA"]]

rdd = spark.sparkContext.parallelize(data)
print('---------------------------')
rdd2 = rdd.map(lambda x: (x[1], 1)).collect()
rdd3 = rdd.filter(lambda x: x[2] in ('BBA', 'BCA'))
op4 = rdd3.collect()
print(op4)

rdd4 = rdd.filter(lambda x: x[0] == 'Ron').count()
print(rdd4)
