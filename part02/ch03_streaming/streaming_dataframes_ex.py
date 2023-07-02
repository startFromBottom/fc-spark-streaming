from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

if __name__ == "__main__":
    ss: SparkSession = SparkSession.builder \
        .master("local[2]") \
        .appName("streaming dataframe examples") \
        .getOrCreate()

    # define schema
    schemas = StructType([
        StructField("ip", StringType(), False),
        StructField("timestamp", StringType(), False),
        StructField("method", StringType(), False),
        StructField("endpoint", StringType(), False),
        StructField("status_code", StringType(), False),
        StructField("latency", IntegerType(), False),  # 단위 : milliseconds
    ])


    def hello_streaming():
        lines = ss.readStream \
            .format("socket") \
            .option("host", "localhost") \
            .option("port", 12345).load()

        lines.writeStream \
            .format("console") \
            .outputMode("append") \
            .trigger(processingTime="2 seconds").start().awaitTermination()


    def read_from_socket():
        # socket 통해서 읽을 때는 option에서 schema를 지정하는 것이 불가능.
        lines = ss.readStream \
            .format("socket") \
            .option("host", "localhost") \
            .option("port", 12345).load()

        cols = ["ip", "timestamp", "method", "endpoint", "status_code", "latency"]

        df = lines.withColumn("ip", F.split(lines['value'], ",").getItem(0)) \
            .withColumn("timestamp", F.split(lines['value'], ",").getItem(1)) \
            .withColumn("method", F.split(lines['value'], ",").getItem(2)) \
            .withColumn("endpoint", F.split(lines['value'], ",").getItem(3)) \
            .withColumn("status_code", F.split(lines['value'], ",").getItem(4)) \
            .withColumn("latency", F.split(lines['value'], ",").getItem(5)).select(cols)

        # filter : status_code = 400, endpoint = "/users"인 row만 필터링
        df = df.filter((df.status_code == "400") & (df.endpoint == "/users"))

        # group by : method, endpoint 별 latency의 최댓값, 최솟값, 평균값 확인
        # 주의)
        # append mode에서는 watermark를 지정하지 않으면, DataFrame에 대한
        # aggregation 연산을 지원하지 않음.
        # 만약 지원한다면, aggregation 연산을 위해 spark는 무한 스트림의 전체 상태를 계속 알고 있어야 함.

        # group_cols = ["method", "endpoint"]
        #
        # df = df.groupby(group_cols) \
        #     .agg(F.max("latency").alias("max_latency"),
        #          F.min("latency").alias("min_latency"),
        #          F.mean("latency").alias("mean_latency"))

        df.writeStream.format("console") \
            .outputMode("append") \
            .start().awaitTermination()


    def read_from_files():
        # file path : directory 만 가능.
        logs_df = ss.readStream \
            .format("csv") \
            .option("header", "false").schema(schemas).load("data/logs")

        logs_df.writeStream \
            .format("console") \
            .outputMode("append") \
            .start() \
            .awaitTermination()


    # hello_streaming()
    read_from_socket()
    # read_from_files()
