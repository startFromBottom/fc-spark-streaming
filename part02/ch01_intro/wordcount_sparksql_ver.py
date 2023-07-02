import pyspark.sql.functions as f
from pyspark.sql import SparkSession

if __name__ == "__main__":
    ss: SparkSession = SparkSession.builder \
        .master("local") \
        .appName("wordCount RDD ver") \
        .getOrCreate()

    # load data
    df = ss.read.text("data/words.txt")

    # transformations
    df = df.withColumn('word', f.explode(f.split(f.col('value'), ' '))) \
        .withColumn("count", f.lit(1)) \
        .groupby("word").sum()

    # show result
    df.show()
