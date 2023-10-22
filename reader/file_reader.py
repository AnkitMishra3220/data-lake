from abc import ABC
from reader.reader import Reader
from pyspark.sql import SparkSession, DataFrame
from exceptions.reader_not_found_exception import ReaderNotFoundException
from exceptions.empty_directory_exception import EmptyDirectoryException
from util import util


class FileReader(Reader, ABC):
    def __init__(self, conf, spark: SparkSession):
        super().__init__(conf, spark)

    def read(self) -> DataFrame:
        if util.get_directory_size(self.conf['dir_path']) == 0:
            raise EmptyDirectoryException(self.conf['dir_path'])
        if self.conf['fileType'] == 'csv':
            return self.spark.read.option('header', self.conf['csv']['header']) \
                .option('inferSchema', self.conf['csv']['inferSchema']).csv(self.conf['dir_path'])
        elif self.conf['fileType'] == 'json':
            return self.spark.read.option("multiline", "true").json(self.conf['dir_path'])
        elif self.conf['fileType'] == 'json':
            return self.spark.read.parquet(self.conf['dir_path'])
        else:
            raise ReaderNotFoundException(self.conf['fileType'])
