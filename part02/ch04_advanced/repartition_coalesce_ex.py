from pyspark.sql import SparkSession

if __name__ == "__main__":
    ss: SparkSession = SparkSession.builder \
        .master("local") \
        .appName("spark_jobs_ex") \
        .getOrCreate()

    df = ss.read.option("header", "true") \
        .option("inferSchema", "true") \
        .csv("data/titanic_data.csv")

    # 1. repartition ex)

    # df = df.repartition(10)
    # df = df.repartition(10, "Age")
    # SparkUI Storage tab에서 각 partition의 사이즈를 확인.
    # df.cache()

    # 2. coalesce ex)
    # coalesce가 narrow transformation 이기 때문에,
    # where, select narrow transformation 도
    # 20개 task가 아닌 5개 task만을 사용하게 됨.
    # 이는 스파크의 병렬성에 악영향을 미칠 수 있음.
    df = df.repartition(20) \
        .where("Pclass = 1") \
        .select("PassengerId", "Survived") \
        .coalesce(5) \
        .where("Age >= 20") \

    # trigger action
    df.count()

    while True:
        pass
