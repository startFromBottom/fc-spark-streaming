from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions \
    import col, to_timestamp, max, min, mean, date_trunc, collect_set, \
    hour, minute, count


def load_data(ss: SparkSession, from_file, schema):
    if from_file:
        return ss.read.schema(schema).csv("data/log.csv")

    log_data_inmemory = [
        ["130.31.184.234", "2023-02-26 04:15:21", "PATCH", "/users", "400", 61],
        ["28.252.170.12", "2023-02-26 04:15:21", "GET", "/events", "401", 73],
        ["180.97.92.48", "2023-02-26 04:15:22", "POST", "/parsers", "503", 17],
        ["73.218.61.17", "2023-02-26 04:16:22", "DELETE", "/lists", "201", 91],
        ["24.15.193.50", "2023-02-26 04:17:23", "PUT", "/auth", "400", 24],
    ]

    return ss.createDataFrame(log_data_inmemory, schema)


if __name__ == "__main__":
    ss: SparkSession = SparkSession.builder \
        .master("local") \
        .appName("log dataframe ex") \
        .getOrCreate()

    # define schema
    fields = StructType([
        StructField("ip", StringType(), False),
        StructField("timestamp", StringType(), False),
        StructField("method", StringType(), False),
        StructField("endpoint", StringType(), False),
        StructField("status_code", StringType(), False),
        StructField("latency", IntegerType(), False),  # 단위 : milliseconds
    ])

    from_file = True

    df = load_data(ss, from_file, fields)

    # 데이터 확인
    df.show()
    # 스키마 확인
    df.printSchema()


    # a) 컬럼 변환
    # a-1) 현재 latency 컬럼의 단위는 millseconds인데, seconds 단위인
    # latency_seconds 컬럼을 새로 만들기.
    def milliseconds_to_seconds(num):
        return num / 1000


    df = df.withColumn(
        "latency_seconds",
        # milliseconds_to_seconds(col("latency"))
        milliseconds_to_seconds(df.latency)
    )

    # a-2) StringType으로 받은 timestamp 컬럼을, TimestampType으로 변경.
    df = df.withColumn("timestamp", to_timestamp(col("timestamp")))

    # b) filter
    # b-1) status_code = 400, endpoint = "/users"인 row만 필터링
    df.filter((df.status_code == "400") & (df.endpoint == "/users"))
    # df.where((df.status_code == "400") & (df.endpoint == "/users"))

    # c) group by
    # c-1) method, endpoint 별 latency의 최댓값, 최솟값, 평균값 확인
    group_cols = ["method", "endpoint"]

    df.groupby(group_cols) \
        .agg(max("latency").alias("max_latency"),
             min("latency").alias("min_latency"),
             mean("latency").alias("mean_latency"))

    # c-2) 분 단위의, 중복을 제거한 ip 리스트, 개수 뽑기
    group_cols = ["hour", "minute"]
    df.withColumn(
        "hour", hour(date_trunc("hour", col("timestamp"))),
    ).withColumn(
        "minute", minute(date_trunc("minute", col("timestamp"))),
    ).groupby(group_cols).agg(collect_set("ip").alias("ip_list"),
                              count("ip").alias("ip_count")) \
        .sort(group_cols) # .explain() -> Physical Plan 확인.
