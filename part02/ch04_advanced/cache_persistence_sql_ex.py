from pyspark.sql import SparkSession
from pyspark.sql.functions import col

if __name__ == "__main__":
    ss: SparkSession = SparkSession.builder \
        .master("local") \
        .appName("cache_persistence_sql_ex") \
        .getOrCreate()

    df = ss.range(1000000) \
        .toDF("id") \
        .withColumn("square", col("id") * col("id"))

    # df.cache()

    # sql
    df.createOrReplaceTempView("dfTable")
    ss.sql("CACHE TABLE dfTable")
    ss.sql("SELECT count(*) FROM dfTable").show()

    # while True:
    #     pass