import random

from pyspark import SparkContext, RDD
from pyspark.sql import SparkSession

if __name__ == "__main__":
    ss: SparkSession = SparkSession.builder \
        .master("local") \
        .appName("check partition count ex") \
        .getOrCreate()

    # 1. rdd
    sc: SparkContext = ss.sparkContext

    num_partitions = 10

    data = sc.parallelize(range(30), 10)
    print(f"number of partitions => {data.getNumPartitions()}")
    print(f"partition structure => {data.glom().collect()}")

    data = data.repartition(5)
    print("=== after repartition ===")
    print(f"number of partitions => {data.getNumPartitions()}")
    print(f"partition structure => {data.glom().collect()}")

    # 2. dataframe
    df = ss.createDataFrame([[i] for i in range(30)]).toDF("id")
    df = df.repartition(10)

    print(f"the number of dataframe partition => {df.rdd.getNumPartitions()}")

    # 3. rdd에서 partitioner 체크.

    rdd1 = sc.parallelize([[i, random.randint(1, 30)] for i in range(30)]).partitionBy(10)
    print(f"partitioner => {rdd1.partitioner}")

    rdd2 = sc.parallelize([[i, random.randint(20, 50)] for i in range(100)]).partitionBy(10)
    print(f"partitioner => {rdd2.partitioner}")

    # join 연산 -> partitioner 정보 유지
    joined_rdd = rdd1.join(rdd2)
    print(f"partitioner => {joined_rdd.partitioner}")

    # map 연산 -> partitioner 정보가 사라짐
    final_rdd = joined_rdd.map(lambda x: [x[1], x[0]])
    print(f"final_data's partitioner => {final_rdd.partitioner}")

    result = final_rdd.collect()

    print(f"result ==> {result}")

    while True:
        pass
