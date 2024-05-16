from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.table("openlibrary.subjects_silver")
explode_authors = df.withColumn("author_explode", F.explode("authors"))

# bridge table
explode_authors.select(F.col("author_explode.name").alias("author"), "title")\
    .repartition(1)\
    .write.csv("/opt/spark/work-dir/reports/authors_and_titles")

books_per_year = explode_authors\
    .groupby("author_explode.name", "first_publish_year")\
    .count()

books_per_year.repartition(1).write.csv("/opt/spark/work-dir/reports/books_by_year")

avg_per_year = books_per_year\
    .groupby("name")\
    .agg(F.avg("count"))\
    .repartition(1)\
    .write.csv("/opt/spark/work-dir/reports/avg_books_by_year")

