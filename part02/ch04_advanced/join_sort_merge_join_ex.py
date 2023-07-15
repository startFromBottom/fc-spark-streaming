import random

from pyspark.sql import SparkSession
from pyspark.sql.functions import asc

# base code: https://github.com/databricks/LearningSparkV2/blob/master/chapter7/scala/src/main/scala/chapter7/SortMergeJoin_7_6.scala

if __name__ == "__main__":
    ss: SparkSession = SparkSession.builder \
        .master("local") \
        .appName("sort_merge_join_ex") \
        .config("spark.sql.join.preferSortMergeJoin", True) \
        .config("spark.sql.autoBroadcastJoinThreshold", -1) \
        .config("spark.sql.defaultSizeInBytes", 100000) \
        .config("spark.sql.shuffle.partitions", 16) \
        .config("spark.sql.adaptive.enabled", False) \
        .getOrCreate()

    states = {
        0: "AZ",
        1: "CO",
        2: "CA",
        3: "TX",
        4: "NY",
        5: "MI",
    }

    items = {
        0: "SKU-0",
        1: "SKU-1",
        2: "SKU-2",
        3: "SKU-3",
        4: "SKU-4",
        5: "SKU-5",
    }

    users_df = ss.createDataFrame([[uid,
                                    f"user_{uid}",
                                    f"user_{uid}@fastcampus.com",
                                    states[random.randint(0, 5)]]
                                   for uid in range(100000)]) \
        .toDF("uid", "login", "email", "user_state")

    orders_df = ss.createDataFrame([[tid,
                                     random.randint(1, 5000),
                                     random.randint(1, 10000),
                                     states[random.randint(0, 5)],
                                     items[random.randint(0, 5)]]
                                    for tid in range(100000)]) \
        .toDF("transaction_id", "login", "uid", "email", "user_state")

    joined_df = orders_df.join(users_df, on="uid")
    joined_df.explain(mode="extended")
    # trigger action
    joined_df.count()

    # join 과정 최적화
    # 최적화 방법
    #  1. uid 로 사전 정렬.
    #  2. 데이터 저장 시, uid로 bucketing.
    users_df.orderBy(asc("uid")) \
        .write.format("parquet") \
        .bucketBy(8, "uid")\
        .mode("overwrite") \
        .saveAsTable("UsersTbl")

    orders_df.orderBy(asc("uid")) \
        .write.format("parquet") \
        .bucketBy(8, "uid")\
        .mode("overwrite") \
        .saveAsTable("OrdersTbl")

    ss.sql("CACHE TABLE usersTbl")
    ss.sql("CACHE TABLE OrdersTbl")

    users_bucket_df = ss.table("UsersTbl")
    orders_bucket_df = ss.table("OrdersTbl")

    joined_bucket_df = users_bucket_df.join(orders_bucket_df,
                                            on="uid")

    joined_bucket_df.explain(mode="extended")
    # trigger action
    joined_bucket_df.count()

    while True:
        pass
