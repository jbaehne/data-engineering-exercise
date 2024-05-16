from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.getOrCreate()
source_table = spark.table("openlibrary.subjects_bronze")
df = spark.readStream.schema(source_table.schema).format("json").load("/opt/spark/work-dir/src/fantasy/")\
    .withColumn("type", F.lit("fantasy"))
query = df.writeStream\
    .format("iceberg")\
    .outputMode("append")\
    .option("mergeSchema", "true")\
    .option("checkPointLocation", "/opt/spark/work-dir/bronze-checkpoints")\
    .trigger(availableNow=True)\
    .toTable("openlibrary.subjects_bronze")

query.processAllAvailable()
query.stop()

