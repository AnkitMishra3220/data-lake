from pyspark import SparkConf
from pyspark.sql import SparkSession, DataFrame
# from pyspark.sql.types import LongType, StringType, StructField, StructType, BooleanType, ArrayType, IntegerType, TimestampType


def create_spark_session() -> SparkSession:
    conf = SparkConf().set("spark.driver.memory", "1g")
    spark_session = SparkSession \
        .builder \
        .master("local[2]") \
        .config(conf=conf) \
        .appName("Write Data to PostgresSQL") \
        .config("spark.jars", "../../lib/postgresql-42.6.0.jar") \
        .getOrCreate()
    return spark_session


def write_data(data_df: DataFrame, table: str, connection_str: str, username: str, password: str):
    properties = {"user": username, "password": password, "driver": "org.postgresql.Driver"}
    data_df.write.mode("append") \
        .jdbc(url=connection_str, table=table, properties=properties)


if __name__ == '__main__':
    spark = create_spark_session()

    target_connection_str = "jdbc:postgresql://localhost:5437/e_commerce"
    target_username = "root"
    target_password = "root"

    ######## To Do ########
    ###### Create customer table with this schema ######

    # StructField(customer_id, BinaryType, false),
    # StructField(customer_unique_id, BinaryType, true),
    # StructField(customer_zip_code_prefix, IntegerType, true)
    # StructField(customer_city, StringType, true),
    # StructField(customer_state, StringType, true)))

    customers_df = spark.read \
        .option("inferSchema", "true") \
        .option("header", True) \
        .csv('../../resources/database_load/customers.csv')

    write_data(
        data_df=customers_df,
        table="customers",
        connection_str=target_connection_str,
        username=target_username,
        password=target_password
    )


    # ********* To Do *************
    # Create and load below tables in Postgres
    # geolocation
    # order_items
    # order_payments
    # order_reviews
    # orders
    # product_category_name_translation
    # products
    # sellers

    # To Do
    # Optimize the code








# import psycopg2
#
# conn = psycopg2.connect(database="e_commerce",
#                         user='postgres', password='pass',
#                         host='pg_container', port='5432'
#                         )
#
# conn.autocommit = True
# cursor = conn.cursor()
#
# sql = '''CREATE TABLE customers(customer_id bytea NOT NULL,
#                                 customer_unique_id bytea,
#                                 customer_zip_code_prefix integer,
#                                 customer_city varchar(50),
#                                 customer_state varchar(50);'''
#
# cursor.execute(sql)
#
# (StructType(StructField(customer_id,BinaryType,false),StructField(customer_unique_id,BinaryType,true),StructField(customer_zip_code_prefix,IntegerType,true),StructField(customer_city,StringType,true),StructField(customer_state,StringType,true)))
# sql2 = '''COPY details(customer_id,
#                        customer_unique_id,
#                        customer_zip_code_prefix,
#                        customer_city,
#                        customer_state)
#           FROM '../resources/database_load/customers.csv'
#           DELIMITER ','
#           CSV HEADER;'''
#
# cursor.execute(sql2)
#
# # sql3 = '''select * from details;'''
# # cursor.execute(sql3)
# # for i in cursor.fetchall():
# #     print(i)
#
# conn.commit()
# conn.close()
