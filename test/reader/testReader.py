import unittest
from reader.file_reader import FileReader
from conf.spark_conf import SparkConf
from exceptions.empty_directory_exception import EmptyDirectoryException


class TestReader(unittest.TestCase):
    SparkConf.create_spark_session()
    spark = SparkConf.spark

    # conf = {'file_type': 'csv', 'dir_path': './empty_dir', 'csv': {'header': True, 'inferSchema': True}}

    def test_empty_folder_read(self):
        conf = {'dir_path': './empty_dir'}
        file_reader = FileReader(conf, self.spark)
        with self.assertRaises(EmptyDirectoryException):
            file_reader.read()


if __name__ == '__main__':
    unittest.main()
