from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MySparkApp").getOrCreate()
print("Apache Spark Version:", spark.version)
print("Hadoop Version:", spark.sparkContext.version)
