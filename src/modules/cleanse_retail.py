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
    spark = SparkSession.builder.appName("Project-Stage-II").config('spark.ui.port', '4050') \
        .config("spark.master", "local").enableHiveSupport().getOrCreate()

    # spark = SparkSession.builder.enableHiveSupport().appName("retail_raw_layer") \
    #     .config('spark.ui.port', '4050').config("spark.master", "local") \
    #     .config('spark.jars.packages',
    #             'net.snowflake:snowflake-jdbc:3.13.23,net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.1').getOrCreate()

    clean_df = spark.read.csv("C:\\Retail_log_project\\src\\internal_files\\raw_layer_retail\\",
        header="True")

    def __init__(self):
        sc = self.spark.sparkContext
        sc.setLogLevel("Error")

    def read_from_local_raw(self):
        try:
            self.clean_df = self.spark.read.csv("C:\\Retail_log_project\\src\\internal_files\\raw_layer_retail\\", header=True, inferSchema=True)

        except Exception as err:
            logging.error('Exception was thrown in connection %s' % err)
            print("Error is {}".format(err))
            sys.exit(1)
        else:
            pass
            # self.clean_df.printSchema()
            # self.clean_df.show()
            # raw_df.show(10, truncate=False)


class clean_layer(Setup):
    def dropDuplicates(self):
        self.dropDuplication = self.clean_df.dropDuplicates()

    def modification(self):
        self.set_one_value_OrderQuantity = self.dropDuplication.withColumn("ProductName", regexp_replace("ProductName", ",", "-"))\
            .withColumn("OrderQuantity",F.when(F.isnull(F.col("OrderQuantity")),1).otherwise(F.col("OrderQuantity")))\
            .withColumn(('SalesAmount'),col('OrderQuantity')*col('UnitPrice'))

        self.set_one_value_OrderQuantity.show()

    # how to check  how many null values is present
    def check_NA(self):
        self.check_null_value = self.set_one_value_OrderQuantity.select(*[
            (
                F.count(F.when((F.isnan(c) | F.col(c).isNull()), c)) if t not in ("timestamp", "date")
                else F.count(F.when(F.col(c).isNull(), c))
            ).alias(c)
            for c, t in self.set_one_value_OrderQuantity.dtypes if
            c in self.set_one_value_OrderQuantity.columns]).show()

    def fill_NA(self):
        self.fill_NA = self.set_one_value_OrderQuantity.na.fill("NA")
        self.fill_NA.show(truncate=False)

    # def sep_size(self):
    #     self.size = self.fill_NA.withColumn("size", split(self.fill_NA["ProductName"], '-') \
    #                                         .getItem(1)) \
    #         .withColumn("ProductName", split(self.fill_NA["ProductName"], '-') \
    #                     .getItem(0))
    #
    #     self.size.show(truncate=False)

    # def group_colur(self):
    #     self.group_colr = self.size.groupBy("Color").count()
    #     self.group_colr.show()
    #
    # def colur_distinct(self):
    #     self.df_colour = self.group_colr.select("Color").distinct()
    #     self.df_colour.show()
    #
    # def colour_list(self):
    #     self.df_list = self.df_colour.rdd.flatMap(lambda x: x).collect()
    #
    # def remove_colr(self):
    #     self.df_removed_color = self.size.withColumn('ProductName',
    #                                                  F.regexp_replace('ProductName', '|'.join(self.df_list), '')) \
    #         .withColumn('ProductName', F.regexp_replace('ProductName', '- ', ''))
    #     self.df_removed_color.show(20, False)

    def clean_layer_save(self):
        self.fill_NA.coalesce(1).write.mode("overwrite").format("csv").save(
            "C:\\Retail_log_project\\src\\internal_files\\clean_layer_retail\\", header="True")

    def show_on_hive(self):
        pass
        sqlContext = HiveContext(self.spark.sparkContext)
        sqlContext.sql('DROP TABLE IF EXISTS Cleansed_layer_retail')
        self.fill_NA.coalesce(1).write.option("mode", "overwrite").saveAsTable('Cleansed_layer_retail')
        self.spark.sql("select count(*) from Cleansed_layer_retail").show()

    # def connect_to_snowflake(self):
    #     self.df_removed_color.coalesce(1).write.format("snowflake").options(**sfOptions).option("dbtable", "{}".format(
    #         r"curated_ordered_details")).mode(
    #         "overwrite").options(header=True).save()


if __name__ == '__main__':
    # clean_l = clean_layer()
    # # clean_l.drop_Freight()
    # clean_l.set_one()
    # clean_l.check_NA()
    # clean_l.fill_NA()
    # clean_l.sep_size()
    # clean_l.group_colur()
    # clean_l.colur_distinct()
    # clean_l.colour_list()
    # clean_l.remove_colr()
    # clean_l.clean_layer_save()
    # clean_l.show_on_hive()
    # # clean_l.connect_to_snowflake()

    # SetUp
    setup = Setup()
    try:
        setup.read_from_local_raw()
    except Exception as e:
        logging.error('Error at %s', 'Reading from local', exc_info=e)
        sys.exit(1)


#Clean
    try:
        cleanLayer = clean_layer()
    except Exception as e:
        logging.error('Error at %s', 'Error at CleanLayer Object Creation', exc_info=e)
        sys.exit(1)


    try:
        cleanLayer.dropDuplicates()
    except Exception as e:
        logging.error('Error at %s', 'Error at dropDuplicates', exc_info=e)
        sys.exit(1)

    try:
        cleanLayer.modification()
    except Exception as e:
        logging.error('Error at %s', 'Error at modification', exc_info=e)
        sys.exit(1)

    try:
        cleanLayer.check_NA()
    except Exception as e:
        logging.error('Error at %s', 'Error at count of null value', exc_info=e)
        sys.exit(1)

    try:
        cleanLayer.fill_NA()
    except Exception as e:
        logging.error('Error at %s', 'Error at NA value', exc_info=e)
        sys.exit(1)

    # try:
    #     cleanLayer.sep_size()
    # except Exception as e:
    #     logging.error('Error at %s', 'Error at sep_size', exc_info=e)
    #     sys.exit(1)

    # try:
    #     cleanLayer.group_colur()
    # except Exception as e:
    #     logging.error('Error at %s', 'Error at group_colur', exc_info=e)
    #     sys.exit(1)
    #
    # try:
    #     cleanLayer.colur_distinct()
    # except Exception as e:
    #     logging.error('Error at %s', 'Error at colur_distinct', exc_info=e)
    #     sys.exit(1)
    #
    # try:
    #     cleanLayer.colour_list()
    # except Exception as e:
    #     logging.error('Error at %s', 'Error at listing of colour', exc_info=e)
    #     sys.exit(1)
    #
    # try:
    #     cleanLayer.remove_colr()
    # except Exception as e:
    #     logging.error('Error at %s', 'Error at removing colour', exc_info=e)
    #     sys.exit(1)

    try:
        cleanLayer.clean_layer_save()
    except Exception as e:
        logging.error('Error at %s', 'Error at write_to_locally', exc_info=e)
        sys.exit(1)

    try:
        cleanLayer.show_on_hive()
    except Exception as e:
        logging.error('Error at %s', 'Error at write_to_hive', exc_info=e)
        sys.exit(1)