from pyspark.sql import SparkSession

if __name__ == "__main__":
    ss: SparkSession = SparkSession.builder \
        .master("local") \
        .appName("spark_jobs_ex") \
        .getOrCreate()

    ss.conf.set("spark.sql.adaptive.enabled", "false")

    ################### job 1 ###################
    # job을 나누는 단위 : action
    # read : action
    df = ss.read.option("header", "true") \
        .option("inferSchema", "true") \
        .csv("data/titanic_data.csv")
    #############################################

    ################### job 2 ###################
    # collect : action

    # stage를 나누는 단위 : wide transformation
    # repartition, groupby : wide transformation
    df = df.repartition(5) \
        .where("Sex = 'male'") \
        .select("Survived", "Pclass", "Fare") \
        .groupby("Survived", "Pclass") \
        .mean()
    print(f"result ===> {df.collect()}")
    #############################################

    # mode = "extended" )
    # Parsed Logical Plan, Analyzed Logical Plan
    # Optimized Logical Plan, Physical Plan 을 모두 확인해 볼 수 있음.
    df.explain(mode="extended")

    # 스크립트가 종료 되지 않도록 하여, spark web UI를 계속 확인할 수 있도록 함.
    while True:
        pass