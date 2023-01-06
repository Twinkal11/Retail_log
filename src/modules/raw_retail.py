from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import HiveContext
from pyspark.sql import functions as F
# from snowflake import sfOptions
import re
import os
import sys
import time
import logging

class Setup:
# class raw_ly():
    # spark = SparkSession.builder.enableHiveSupport().appName("retail_raw_layer") \
    #  .config('spark.ui.port', '4050').config("spark.master", "local") \
    #     .config('spark.jars.packages',
    #      'net.snowflake:snowflake-jdbc:3.13.23,net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.1').getOrCreate()
    # df = spark.read.format("csv").option("header", "true").load("C:\\Retail_log_project\\input_file\\Retail.csv")

    spark = SparkSession.builder.appName("retail_raw_layer") \
        .config('spark.ui.port', '4050').config("spark.master", "local") \
        .enableHiveSupport().getOrCreate()

    df = spark.read.format("csv").option("header", "true").load("C:\\Retail_log_project\\input_file\\Retail_result.csv")

    def __init__(self):
        sc = self.spark.sparkContext
        sc.setLogLevel("Error")

    def read_from_s3_sink(self):
        try:
            self.df = self.spark.read.text("C:\\Retail_log_project\\input_file\\Retail_result.csv")

        except Exception as err:
            logging.error('Exception was thrown in connection %s' % err)
            print("Error is {}".format(err))
            sys.exit(1)
        else:
            pass
            # self.df.printSchema()


class Cleaning(Setup):
    def read_file(self):
        # df = self.spark.read.format("csv").option("header", "true").load("C:\\Retail_log_project\\input_file\\Retail.csv")

        self.df = self.df.withColumn("id", monotonically_increasing_id())
        self.df1 = self.df.select("id", "OrderNumber", "ProductName", "Color", "Category", "Subcategory", "ListPrice",
                                  "Orderdate", "Duedate", "Shipdate", "PromotionName", "SalesRegion", "OrderQuantity",
                                  "UnitPrice", "SalesAmount", "DiscountAmount", "TaxAmount", "Freight")
        self.df1.show()

    def date_type(self):
        self.date_datatype = self.df1.withColumn('Orderdate', F.to_date(F.col('Orderdate'), 'M/d/yyyy')) \
            .withColumn('Duedate', F.to_date(F.col('Duedate'), 'M/d/yyyy')) \
            .withColumn('Shipdate', F.to_date(F.col('Shipdate'), 'M/d/yyyy'))

    def set_datatype(self):
        self.dataSchema = self.date_datatype.withColumn("OrderNumber", col("OrderNumber").cast(StringType())) \
            .withColumn("ProductName", col("ProductName").cast(StringType())) \
            .withColumn("Color", col("Color").cast(StringType())) \
            .withColumn("Category", col("Category").cast(StringType())) \
            .withColumn("ListPrice", col("ListPrice").cast(DoubleType())) \
            .withColumn("Orderdate", col("Orderdate").cast(DateType())) \
            .withColumn("Duedate", col("Duedate").cast(DateType())) \
            .withColumn("Shipdate", col("Shipdate").cast(DateType())) \
            .withColumn("PromotionName", col("PromotionName").cast(StringType())) \
            .withColumn("SalesRegion", col("SalesRegion").cast(StringType())) \
            .withColumn("OrderQuantity", col("OrderQuantity").cast(IntegerType())) \
            .withColumn("UnitPrice", col("UnitPrice").cast(DoubleType())) \
            .withColumn("SalesAmount", col("SalesAmount").cast(DoubleType())) \
            .withColumn("DiscountAmount", col("DiscountAmount").cast(DoubleType())) \
            .withColumn("TaxAmount", col("TaxAmount").cast(DoubleType())) \
            .withColumn("Freight", col("Freight").cast(DoubleType()))

        self.dataSchema.show()
        # self.dataSchema.printSchema()

    def raw_layer_save(self):
        self.dataSchema.coalesce(1).write.mode("overwrite").format("csv").save(
            "C:\\Retail_log_project\\src\\internal_files\\raw_layer_retail\\", header="True", mode='overwrite')

    def show_On_Hive(self):
        pass
        sqlContext = HiveContext(self.spark.sparkContext)
        sqlContext.sql('DROP TABLE IF EXISTS Raw_layer_retail')
        self.dataSchema.coalesce(1).write.option("mode", "overwrite").saveAsTable('Raw_layer_retail')
        self.spark.sql("select count(*) from Raw_layer_retail").show()

    # def connect_to_snowflake(self):
    #     self.dataSchema.coalesce(1).write.format("snowflake").options(**sfOptions).option("dbtable", "{}".format(
    #         r"raw_ordered_details")).mode(
    #         "overwrite").options(header=True).save()


if __name__ == '__main__':
    # raw = raw_ly()
    # raw.read_file()
    # raw.date_type()
    # raw.set_datatype()
    # raw.raw_layer_save()
    # raw.show_On_Hive()
    # # raw.connect_to_snowflake()
    # Setup
    setup = Setup()
    try:
        setup.read_from_s3_sink()
    except Exception as e:
        logging.error('Error at %s', 'Reading from Local', exc_info=e)
        sys.exit(1)

        # Clean
    try:
        clean = Cleaning()
    except Exception as e:
        logging.error('Error at %s', 'Cleaning Object Creation', exc_info=e)
        sys.exit(1)

    try:
        clean.read_file()
    except Exception as e:
        logging.error('Error at %s', 'read_file', exc_info=e)
        sys.exit(1)

    try:
        clean.date_type()
    except Exception as e:
        logging.error('Error at %s', 'date_type', exc_info=e)
        sys.exit(1)

    try:
        clean.set_datatype()
    except Exception as e:
        logging.error('Error at %s', 'set_datatype', exc_info=e)
        sys.exit(1)

    try:
        clean.raw_layer_save()
    except Exception as e:
        logging.error('Error at %s', 'raw_layer_save', exc_info=e)
        sys.exit(1)

    try:
        clean.show_On_Hive()
    except Exception as e:
        logging.error('Error at %s', 'show_On_Hive', exc_info=e)
        sys.exit(1)