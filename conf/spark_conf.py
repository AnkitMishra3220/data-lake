from pyspark.sql import SparkSession, DataFrame
from pyspark import SparkConf


class SparkConf:
    spark: SparkSession = None

    @classmethod
    def create_spark_session(cls):
        # config = SparkConf().set("spark.driver.memory", "1g")
        spark = SparkSession \
            .builder \
            .master("local[2]") \
            .appName("Write Data to PostgresSQL") \
            .config("spark.jars", "../../lib/postgresql-42.6.0.jar") \
            .getOrCreate()
