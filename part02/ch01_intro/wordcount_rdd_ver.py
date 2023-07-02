from pyspark import SparkContext, RDD
from pyspark.sql import SparkSession

if __name__ == "__main__":
    ss: SparkSession = SparkSession.builder \
        .master("local") \
        .appName("wordCount RDD ver") \
        .getOrCreate()
    sc: SparkContext = ss.sparkContext

    # load data
    text_file: RDD[str] = sc.textFile("data/words.txt")

    # transformations
    counts = text_file.flatMap(lambda line: line.split(" ")) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda count1, count2: count1 + count2)

    # action
    output = counts.collect()

    # show result
    for (word, count) in output:
        print("%s: %i" % (word, count))
