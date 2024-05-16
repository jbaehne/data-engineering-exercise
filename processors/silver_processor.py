from pyspark.sql import SparkSession
import pyspark.sql.functions as F


def process(df, batch_id):
    df.createOrReplaceTempView("fantasy_batch")
    sql = """
        merge into openlibrary.subjects_silver dest 
        using (
            select *
            from (
                select *, row_number() over (partition by key, type order by key) as rownum
                from fantasy_batch
            ) batch
            where batch.rownum = 1
        ) source
        on dest.key=source.key
        and dest.type=source.type
        when matched then update set *
        when not matched then insert *
    """
    df.sparkSession.sql(sql)

spark = SparkSession.builder.getOrCreate()

bronze_stream = spark.readStream\
    .format("iceberg") \
    .option("stream-from-timestamp", "1715731200000")\
    .table("openlibrary.subjects_bronze")

query = bronze_stream.writeStream\
    .option("checkPointLocation", "/opt/spark/work-dir/silver-checkpoints")\
    .outputMode("append")\
    .foreachBatch(process) \
    .trigger(availableNow=True)\
    .start()\
    .awaitTermination()

# query.processAllAvailable()
# query.stop()
