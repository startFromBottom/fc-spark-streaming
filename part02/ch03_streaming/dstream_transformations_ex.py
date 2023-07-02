import os
from collections import namedtuple

from pyspark import SparkContext, RDD
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext, DStream

columns = ["Ticker", "Date", "Open", "High",
           "Low", "Close", "AdjClose", "Volume"]
Finance = namedtuple("Finance",
                     columns)

if __name__ == "__main__":
    sc: SparkContext = SparkSession.builder \
        .master("local[2]") \
        .appName("DStream transformations ex") \
        .getOrCreate().sparkContext

    ssc = StreamingContext(sc, 5)


    def read_finance():
        # 1. map
        def parse(line: str):
            arr = line.split(",")
            return Finance(*arr)

        return ssc.socketTextStream("localhost", 12345) \
            .map(parse)


    finance_stream: DStream[Finance] = read_finance()


    # 2. filter

    def filter_nvda():
        finance_stream.filter(lambda f: f.Ticker == "NVDA").pprint()


    def filter_volume():
        finance_stream.filter(lambda f: int(f.Volume) > 100000000).pprint()


    # 3. reduce by, group by
    # works per BATCH
    def count_dates_ticker():
        finance_stream.map(lambda f: (f.Ticker, 1)) \
            .reduceByKey(lambda a, b: a + b).pprint()


    def group_by_dates_volume():
        finance_stream.map(lambda f: (f.Date, int(f.Volume))) \
            .groupByKey().mapValues(sum).pprint()


    # 4. foreach RDD
    def save_to_json():
        def foreach_func(rdd: RDD):
            if rdd.isEmpty():
                print("RDD is empty")
                return
            df = rdd.toDF(columns)
            # absolute path
            dir_path = "/Users/eomhyeonho/Workspace/spark-practices/8_streaming/data/stocks/outputs"
            n_files = len(os.listdir(dir_path))
            path = f"{dir_path}/finance-{n_files}.json"
            df.write.json(path)
            print(f"num-partitions => {df.rdd.getNumPartitions()}")
            print("write completed")

        finance_stream.foreachRDD(foreach_func)


    # read_finance().pprint()
    # filter_nvda()
    # filter_volume()
    # count_dates_ticker()
    # group_by_dates_volume()
    save_to_json()

    ssc.start()
    ssc.awaitTermination()
