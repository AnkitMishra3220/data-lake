from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.master('local[2]').\
    getOrCreate()


# Create green_trip dataframe
green_trip_df = spark.read.parquet('../resources/files/green_tripdata_2023-01.parquet')
# green_trip_df.repartition(2)
green_trip_df.show()
# green_trip_df.printSchema()
# Create yellow trip dataframe
# yellow_trip_df = spark.read.parquet('../resources/files/yellow_tripdata_2023-01.parquet')
# # yellow_trip_df.show()
# yellow_trip_df.printSchema()
# # Total Trip Amount of Yellow Texi
# print('******** Total Trip Amount *********')
# yellow_trip_df.select(sum('tip_amount').alias('total_trip_amount')).show()
# yellow_trip_df.agg(sum('tip_amount').alias('total_trip_amount')).show()
# #
# # Find sum of total amount of each vendor for Yellow Texi
# print('******** sum of total amount of each vendor for Yellow Texi ********')
# yellow_trip_df.groupBy('VendorID').sum('total_amount').show()
#
# # List all distinct payment types of Green Texi
# print('******** distinct payment types *************')
# green_trip_df.select('payment_type').distinct().show()
#
# # Find sum of fare amount of pickup date 2023-01-10 for Yellow Texi
# yellow_trip_df.filter(to_date('tpep_pickup_datetime') == '2023-01-10').select(sum('fare_amount')
#             .alias('total_fare_amount_of_2023_01_01')).show()
#
#
# # Find sum of fare amount of each vendor
# print('sum of fare amount of each vendor')
# yellow_trip_df.select('fare_amount', 'VendorID').union(green_trip_df.select('fare_amount', 'VendorID'))\
#     .groupBy('VendorID').sum('fare_amount').show()
#
# # List sum of total amount whose pickup date is different from drop date
# yellow_trip_df.filter(to_date('tpep_pickup_datetime') != to_date('tpep_dropoff_datetime'))\
#     .select(sum('total_amount').alias('total amount whose pickup date is different from drop date')).show()





hold = input('holding spark session')








