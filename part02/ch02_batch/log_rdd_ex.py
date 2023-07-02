from typing import List
from datetime import datetime

from pyspark import SparkContext, RDD
from pyspark.sql import SparkSession

if __name__ == "__main__":
    ss: SparkSession = SparkSession.builder \
        .master("local") \
        .appName("rdd examples ver") \
        .getOrCreate()
    sc: SparkContext = ss.sparkContext

    log_rdd: RDD[str] = sc.textFile("data/log.txt")

    # check count
    print(f"count of RDD ==> {log_rdd.count()}")

    # print each row
    log_rdd.foreach(lambda v: print(v))

    # a) map
    # a-1) log.txt 의 각 행을 List[str] 형태로 받아오기.
    def parse_line(row: str):
        return row.strip().split(" | ")

    parsed_log_rdd: RDD[List[str]] = log_rdd.map(parse_line)

    # b) filter
    # b-1) status code가 404인 log만 필터링.
    def get_only_404(row: List[str]):
        status_code = row[3]
        return status_code == '404'

    rdd_404 = parsed_log_rdd.filter(get_only_404)

    # b-2) status code가 정상인 경우(2xx)인 log만 필터링.
    def get_only_2xx(row: List[str]):
        status_code = row[3]
        return status_code.startswith("2")

    rdd_normal = parsed_log_rdd.filter(get_only_2xx)

    # b-3) post 요청이고 /playbooks api인 log만 필터링.
    def get_post_request_and_playbooks_api(row: List[str]):
        log = row[2].replace("\"", "")
        return log.startswith("POST") and "/playbooks" in log

    rdd_post_playbooks = parsed_log_rdd \
        .filter(get_post_request_and_playbooks_api)

    # c) reduce
    # c-1) API method (POST/GET/PUT/PATCH/DELETE) 별 개수 출력.
    def extract_api_method(row: List[str]):
        api_log = row[2].replace("\"", "")
        api_method = api_log.split(" ")[0]
        return api_method, 1

    rdd_count_by_api_method = parsed_log_rdd.map(extract_api_method) \
        .reduceByKey(lambda c1, c2: c1 + c2) \
        .sortByKey()

    # c-2) 분 단위 별 요청 횟수 출력.
    def extract_hour_and_minute(row: List[str]) -> tuple[str, int]:
        timestamp = row[1].replace("[", "").replace("]", "")
        date_format = "%d/%b/%Y:%H:%M:%S"
        date_time_obj = datetime.strptime(timestamp, date_format)
        return f"{date_time_obj.hour}:{date_time_obj.minute}", 1

    rdd_count_by_minute = parsed_log_rdd.map(extract_hour_and_minute) \
        .reduceByKey(lambda c1, c2: c1 + c2) \
        .sortByKey()

    # d) group by
    # d-1) status code, api method 별 ip 리스트 출력
    def extract_cols(row: List[str]) -> tuple[str, str, str]:
        ip = row[0]
        status_code = row[3]
        api_log = row[2].replace("\"", "")
        api_method = api_log.split(" ")[0]

        return status_code, api_method, ip

    # reduceByKey 사용
    # parsed_log_rdd.map(extract_cols)\
    #     .map(lambda x: ((x[0], x[1]), x[2]))\
    #     .reduceByKey(lambda i1, i2: f"{i1},{i2}") \
    #     .map(lambda row: (row[0], row[1].split(",")))

    # groupby 사용
    parsed_log_rdd.map(extract_cols) \
        .map(lambda x: ((x[0], x[1]), x[2])) \
        .groupByKey().mapValues(list)  # 큰 데이터 셋에서 groupByKey를 하는 것은 성능적으로 좋지 않음.
