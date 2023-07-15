from pyspark import SparkContext, StorageLevel
from pyspark.sql import SparkSession

if __name__ == "__main__":
    ss: SparkSession = SparkSession.builder \
        .master("local") \
        .appName("cache_persistence_ex") \
        .getOrCreate()
    sc: SparkContext = ss.sparkContext

    data = [i for i in range(1, 10000000)]

    rdd = sc.parallelize(data).map(lambda v: [v, v ** 2, v ** 3])

    # rdd.persist(StorageLevel.MEMORY_ONLY) # rdd.cache()와 동일
    # rdd.persist(StorageLevel.DISK_ONLY)
    # rdd.persist(StorageLevel.MEMORY_ONLY_2)
    # rdd.cache()
    rdd.count()
    rdd.count()
    rdd.count()

    # 스크립트가 종료 되지 않도록 하여, spark web UI를 계속 확인할 수 있도록 함.
    # while True:
    #     pass
