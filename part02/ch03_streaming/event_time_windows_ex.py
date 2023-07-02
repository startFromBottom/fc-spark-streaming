from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

if __name__ == "__main__":
    ss: SparkSession = SparkSession.builder \
        .master("local[2]") \
        .appName("Event time windows ex") \
        .getOrCreate()

    domain_traffic_schema = StructType([
        StructField("id", StringType(), False),
        StructField("domain", StringType(), False),
        StructField("count", IntegerType(), False),
        StructField("time", TimestampType(), False),
    ])


    def read_traffics_from_socket():
        return ss.readStream \
            .format("socket") \
            .option("host", "localhost") \
            .option("port", 12345).load() \
            .select(F.from_json(F.col("value"),
                                domain_traffic_schema)
                    .alias("traffic")).selectExpr("traffic.*")


    def aggregate_traffic_counts_by_sliding_window():
        traffics_df = read_traffics_from_socket()

        # window() : struct type column {start, end} 2개의 필드를 가지고 있음.
        window_by_hours = traffics_df.groupby(F.window(F.col("time"),
                                                       windowDuration="2 hours",
                                                       slideDuration="1 hour").alias("time")) \
            .agg(F.sum("count").alias("total_count")) \
            .select(
            F.col("time").getField("start").alias("start"),
            F.col("time").getField("end").alias("end"),
            F.col("total_count")
        ).orderBy(F.col("start"))

        window_by_hours.writeStream.format("console") \
            .outputMode("complete") \
            .start() \
            .awaitTermination()


    def aggregate_traffic_counts_by_tumbling_window():
        # tumbling window : window 간에 겹치는 부분이 없음.
        # sliding duration == window duration.
        traffics_df = read_traffics_from_socket()

        # window() : struct type column {start, end} 2개의 필드를 가지고 있음.
        window_by_hours = traffics_df.groupby(F.window(F.col("time"), "1 hour").alias("time")) \
            .agg(F.sum("count").alias("total_count")) \
            .select(
            F.col("time").getField("start").alias("start"),
            F.col("time").getField("end").alias("end"),
            F.col("total_count")
        ).orderBy(F.col("start"))

        window_by_hours.writeStream.format("console") \
            .outputMode("complete") \
            .start() \
            .awaitTermination()

        pass


    """
    Q) 매 시간마다 traffic이 가장 많은 도메인 출력
    """


    def read_traffics_from_file():
        return ss.readStream.schema(domain_traffic_schema) \
            .json("data/traffics")


    def find_larget_traffic_domain_per_hour():
        traffics_df = read_traffics_from_file()

        largest_traffic_domain = \
            traffics_df.groupby(F.col("domain"), F.window(F.col("time"), "1 hour").alias("hour")) \
                .agg(F.sum("count").alias("total_count")) \
                .select(
                F.col("hour").getField("start").alias("start"),
                F.col("hour").getField("end").alias("end"),
                F.col("domain"),
                F.col("total_count")
            ).orderBy(F.col("hour"), F.col("total_count").desc())

        # 각 window 별 최대의 도메인 1개만 구하기 위한 코드.
        # -> spark streaming에서, time이 아닌 필드로 window를 만드는 것은 허용되지 않음.
        # window = Window.partitionBy(F.col("hour")).orderBy(F.col("hour"), F.col("total_count").desc())
        # largest_traffic_domain = df.select("*", F.rank().over(window).alias('rank'))\
        #     .filter(F.col("rank") == 1)

        largest_traffic_domain.writeStream \
            .format("console").outputMode("complete") \
            .start() \
            .awaitTermination()


    # aggregate_traffic_counts_by_sliding_window()
    # aggregate_traffic_counts_by_tumbling_window()
    find_larget_traffic_domain_per_hour()
