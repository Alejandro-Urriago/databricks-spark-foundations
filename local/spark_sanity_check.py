from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("spark-sanity-check")
    .master("local[*]")
    .getOrCreate()
)

df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "value"])
print("Row count:", df.count())
df.show()

spark.stop()
