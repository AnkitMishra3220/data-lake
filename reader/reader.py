from abc import ABC, abstractmethod
from pyspark.sql import SparkSession, DataFrame


class Reader(ABC):
    def __init__(self, conf, spark: SparkSession):
        self.conf = conf
        self.spark = spark

    @abstractmethod
    def read(self, spark: SparkSession, conf) -> DataFrame:
        pass
