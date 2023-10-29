from pyspark import SparkConf
from pyspark.sql import SparkSession, DataFrame
from time import sleep
from pyspark.sql.functions import expr
from pyspark.sql.functions import window, col


def create_spark_session() -> SparkSession:
    conf = SparkConf().set("spark.driver.memory", "1g")
    spark_session = SparkSession \
        .builder \
        .master("local[2]") \
        .config(conf=conf) \
        .appName("Read From File Source") \
        .getOrCreate()
    return spark_session


if __name__ == '__main__':
    spark = create_spark_session()
    activity_data = spark.read.json('./resources/activity_data/')
    activity_data_schema = activity_data.schema

    activity_data_stream = spark.readStream.schema(activity_data_schema) \
        .option("maxFilesPerTrigger", 1).json('./resources/activity_data/')

    ## Set Conf

    spark.conf.set("spark.sql.shuffle.partitions", 5)

    activity_count = activity_data_stream.groupby("gt").count()

    # activity_query = activity_count.writeStream \
    #     .queryName("activity_counts") \
    #     .format("memory") \
    #     .outputMode("complete") \
    #     .start()
    #
    # for x in range(5):
    #     print("====== activity count ========")
    #     spark.sql("SELECT * FROM activity_counts").show()
    #     sleep(1)

    activity_data_stream_transformation = activity_data_stream \
        .withColumn("stairs", expr("gt like '%stairs%'")) \
        .where("gt is not null") \
        .writeStream \
        .queryName("test_transformation").format("memory").outputMode("append").start()

    for x in range(5):
        print("===== Test Transformation =============")
        spark.sql("SELECT * FROM test_transformation").show()
        sleep(1)

    activity_data_stream_transformation.awaitTermination()
    # activity_query.awaitTermination()

    activity_data_stream = activity_data_stream \
        .selectExpr("*", "cast(cast(Creation_Time as double)/1000000000 as timestamp) as event_time")

    activity_data_stream_window = activity_data_stream \
        .groupby(window(col("event_time"), "3 minutes")).count() \
        .writeStream.queryName("event_per_window") \
        .format("memory").outputMode("complete") \
        .start()

    for x in range(5):
        print("======== Event Per Window ===== ")
        spark.sql("SELECT * FROM event_per_window").show()
        sleep(1)

    activity_data_stream_window.awaitTermination()

    # Perform an aggregation on multiple columns
    activity_data_stream_window_mul_cols = activity_data_stream \
        .groupBy(window(col("event_time"), "10 minutes"), "User").count() \
        .writeStream \
        .queryName("py-events_per_window") \
        .format("memory") \
        .outputMode("complete") \
        .start()

    activity_data_stream_window_mul_cols.awaitTermination()

    # 10-minute windows, starting every five minutes.

    sliding_window = activity_data_stream \
        .groupBy(window(col("event_time"), "10 minutes", "5 minutes")) \
        .count() \
        .writeStream \
        .queryName("pyevents_per_window") \
        .format("memory") \
        .outputMode("complete") \
        .start()

    sliding_window.awaitTermination()
