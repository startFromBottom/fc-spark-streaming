from pyspark.sql import SparkSession
import pyspark.sql.functions as F

if __name__ == "__main__":
    ss: SparkSession = SparkSession.builder \
        .master("local[2]") \
        .appName("Processing time windows") \
        .getOrCreate()


    def read_from_socket():
        # socket 통해서 읽을 때는 option에서 schema를 지정하는 것이 불가능.
        lines = ss.readStream \
            .format("socket") \
            .option("host", "localhost") \
            .option("port", 12345).load()

        cols = ["time", "count"]
        df = lines.withColumn(cols[0], F.split(lines['value'], ",").getItem(0)) \
            .withColumn(cols[1], F.split(lines['value'], ",").getItem(1))

        # .withWatermark("time", "10 seconds")
        aggr_df = df.select(F.to_timestamp(F.col("time")).alias("time"), F.col("count")) \
            .withWatermark("time", "10 minutes") \
            .groupby(F.window(F.col("time"), "5 minutes").alias("time")) \
            .agg(F.sum("count").alias("total_count")).select(
            F.col("time").getField("start").alias("start"),
            F.col("time").getField("end").alias("end"),
            F.col("total_count")
        )

        aggr_df.writeStream.format("console") \
            .outputMode("update") \
            .start().awaitTermination()


    read_from_socket()
