"""
from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import HiveContext
from pyspark.sql import functions as F

from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number
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

    curated = spark.read.csv("C:\\Retail_log_project\\src\\internal_files\\clean_layer_retail\\", header="True")

    def __init__(self):
        sc = self.spark.sparkContext
        sc.setLogLevel("Error")

    def read_from_local_clean(self):
        try:
             self.curated = self.spark.read.csv("C:\\Retail_log_project\\src\\internal_files\\clean_layer_retail\\", header=True, inferSchema=True)

        except Exception as err:
            logging.error('Exception was thrown in connection %s' % err)
            print("Error is {}".format(err))
            sys.exit(1)
        else:
            pass
            # self.clean_df.printSchema()
            # self.clean_df.show()
            # raw_df.show(10, truncate=False)



class curated_layer(Setup):
    def Discount_present_y_N(self):
        self.discount_present = self.curated.withColumn('DiscountAmount_Y/N', when(col('DiscountAmount') == 0.0, "NO").otherwise('YES'))
        self.discount_present.show()



    def curated(self):
        self.curated_l = self.discount_present.withColumn("salesamt-pstded",
                                                         F.col("SalesAmount") - F.col("TaxAmount") - F.col("Freight")-F.col("DiscountAmount"))
        self.curated_l.show()


    def agg_cat(self):
        self.category = self.curated_l.groupBy("ProductName").agg(F.sum("salesamt-pstded").alias("salesamt-pstded"), F.sum("SalesAmount").alias("SalesAmount")).orderBy(F.col("salesamt-pstded").desc()).limit(10)



    def products(self):
        self.products= self.curated_l .groupBy("ProductName", "Category").agg(
            F.sum("salesamt-pstded").alias("salesamt-pstded")).orderBy(F.col("salesamt-pstded").desc()).limit(10)

    def curated_layer_data(self):
        self.curated_l.coalesce(1).write.mode("overwrite").format('csv').option("header", True).save(
            "C:\\Retail_log_project\\src\\internal_files\\curated_layer_Retail_result\\curated_layer_Retail_result\\curated_layer_Retail_result.csv")

        # self.curated_l.coalesce(1).write.format("snowflake").options(**sfOptions).option("dbtable", "{}".format(
        #     r"curated_ordered_details")).mode(
        #     "overwrite").options(header=True).save()

    def write_for_category(self):
        self.category.coalesce(1).write.mode("overwrite").format('csv').option("header", True).save(
            "C:\\Retail_log_project\\src\\internal_files\\curated_layer_Retail_result\\category\\category.csv")

        # self.agg_OrderQuantity_SalesAmount.coalesce(1).write.format("snowflake").options(**sfOptions).option("dbtable", "{}".format(
        #     r"category_ordered_details")).mode(
        #     "overwrite").options(header=True).save()


    def write_for_product(self):
        self.products.coalesce(1).write.mode("overwrite").format('csv').option("header", True).save(
            "C:\\Retail_log_project\\src\\internal_files\\curated_layer_Retail_result\\product\\product.csv")

        # self.curated_df.coalesce(1).write.format("snowflake").options(**sfOptions).option("dbtable", "{}".format(
        #     r"sub_category_ordered_details")).mode(
        #     "overwrite").options(header=True).save()

    def hive_table_for_sold_categorywise(self):
        pass
        sqlContext = HiveContext(self.spark.sparkContext)
        sqlContext.sql('DROP TABLE IF EXISTS category')
        self.category.write.option("mode", "overwrite").saveAsTable('category')
        self.spark.sql("select count(*) from sold_category_wise").show()

    def hive_table_for_cat_productwise(self):
        pass
        sqlContext = HiveContext(self.spark.sparkContext)
        sqlContext.sql('DROP TABLE IF EXISTS product')
        self.products.write.option("mode", "overwrite").saveAsTable('product')
        self.spark.sql("select count(*) from hive_Top_ten_subcategory").show()






if __name__ == '__main__':
    # c = curated_layer()
    # c.Discount_present_y_N()
    # c.Drop_Discount()
    # c.salesamount_Freight()
    # c.agg_cat()
    # c.categorywise_agg()
    # c.curated_layer_data()
    # c.sold_category_wise_SAVE()
    # c.top_ten_subcategory_wise()
    # c.hive_table_for_sold_category_wise_product()
    # c.hive_table_for_cat_subcatagorywise()

    # SetUp
    setup = Setup()
    try:
        setup.read_from_local_clean()
    except Exception as e:
        logging.error('Error at %s', 'Reading from local', exc_info=e)
        sys.exit(1)

    # Clean
    try:
        cleanLayer = curated_layer()
    except Exception as e:
        logging.error('Error at %s', 'Error at CuratedLayer Object Creation', exc_info=e)
        sys.exit(1)

    try:
        cleanLayer.Discount_present_y_N()
    except Exception as e:
        logging.error('Error at %s', 'Error at Discount_present_y_N', exc_info=e)
        sys.exit(1)



    try:
        cleanLayer.curated()
    except Exception as e:
        logging.error('Error at %s', 'Error at salesamount_Freight', exc_info=e)
        sys.exit(1)

    try:
        cleanLayer.agg_cat()
    except Exception as e:
        logging.error('Error at %s', 'Error at agg_cat', exc_info=e)
        sys.exit(1)

    try:
        cleanLayer.products()
    except Exception as e:
        logging.error('Error at %s', 'Error at products', exc_info=e)
        sys.exit(1)

    try:
        cleanLayer.curated_layer_data()
    except Exception as e:
        logging.error('Error at %s', 'Error at write_to_curatedlayerdata', exc_info=e)
        sys.exit(1)

    try:
        cleanLayer.write_for_category()
    except Exception as e:
        logging.error('Error at %s', 'Error at sold_category_wise_SAVE', exc_info=e)
        sys.exit(1)

    try:
        cleanLayer.write_for_product()
    except Exception as e:
        logging.error('Error at %s', 'Error at write_for_product', exc_info=e)
        sys.exit(1)

    try:
        cleanLayer.hive_table_for_sold_categorywise()
    except Exception as e:
        logging.error('Error at %s', 'Error at hive_table_for_sold_category_wise_product', exc_info=e)
        sys.exit(1)

    try:
        cleanLayer.hive_table_for_cat_productwise()
    except Exception as e:
        logging.error('Error at %s', 'Error at hive_table_for_cat_productwise', exc_info=e)
        sys.exit(1)


"""
from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import HiveContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number
import re
import os
import sys
import time
import logging

class Setup:
    spark = SparkSession.builder.appName("Project-Stage-II").config('spark.ui.port', '4050') \
        .config("spark.master", "local").enableHiveSupport().getOrCreate()

    # Read in the CSV file and create a DataFrame
    curated_df = spark.read.csv("C:\\Retail_log_project\\src\\internal_files\\clean_layer_retail\\", header=True, inferSchema=True)

    def __init__(self):
        sc = self.spark.sparkContext
        sc.setLogLevel("Error")

    def read_from_local_clean(self):
        try:
            self.clean_df = self.spark.read.csv("C:\\Retail_log_project\\src\\internal_files\\clean_layer_retail\\", header=True, inferSchema=True)

        except Exception as err:
            logging.error('Exception was thrown in connection %s' % err)
            print("Error is {}".format(err))
            sys.exit(1)
        else:
            pass

class curated_layer(Setup):
    def Discount_present_y_N(self):
        # Modify the DataFrame by adding a new column
        self.discount_present = self.curated_df.withColumn('DiscountAmount_Y/N', when(col('DiscountAmount') == 0.0, "NO").otherwise('YES'))
        self.discount_present.show()

    def curated(self):
        self.curated_l = self.discount_present.withColumn("salesamt-pstded",
                                                         F.col("SalesAmount") - F.col("TaxAmount") - F.col("Freight")-F.col("DiscountAmount"))
        self.curated_l.show()

    def agg_cat(self):
        self.category = self.curated_l.groupBy("ProductName").agg(F.sum("salesamt-pstded").alias("salesamt-pstded"), F.sum("SalesAmount").alias("SalesAmount")).orderBy(F.col("salesamt-pstded").desc()).limit(10)
        self.category.show()
    def products(self):
        self.products= self.curated_l .groupBy("ProductName", "Category").agg(
            F.sum("salesamt-pstded").alias("salesamt-pstded")).orderBy(F.col("salesamt-pstded").desc()).limit(10)
        self.products.show()

    def curated_layer_data(self):
        self.curated_l.coalesce(1).write.mode("overwrite").format('csv').option("header", True).save(
            "C:\\Retail_log_project\\src\\internal_files\\curated_layer_Retail_result\\curated_layer_Retail_result\\curated_layer_Retail_result.csv")




    # def curated_layer_data(self):
    #     self.curated_l.coalesce(1).write.mode("overwrite").format('csv').option("header", True).save(
    #         "C:\\Retail_log_project\\src\\internal_files\\curated_layer_Retail_result\\curated_layer_Retail_result\\curated_layer_Retail_result.csv")
    #
    #     # self.curated_l.coalesce(1).write.format("snowflake").options(**sfOptions).option("dbtable", "{}".format(
    #     #     r"curated_ordered_details")).mode(
    #     #     "overwrite").options(header=True).save()

    def write_for_category(self):
        self.category.coalesce(1).write.mode("overwrite").format('csv').option("header", True).save(
            "C:\\Retail_log_project\\src\\internal_files\\curated_layer_Retail_result\\category\\category.csv")

        # self.agg_OrderQuantity_SalesAmount.coalesce(1).write.format("snowflake").options(**sfOptions).option("dbtable", "{}".format(
        #     r"category_ordered_details")).mode(
        #     "overwrite").options(header=True).save()


    def write_for_product(self):
        self.products.coalesce(1).write.mode("overwrite").format('csv').option("header", True).save(
            "C:\\Retail_log_project\\src\\internal_files\\curated_layer_Retail_result\\product\\product.csv")

        # self.curated_df.coalesce(1).write.format("snowflake").options(**sfOptions).option("dbtable", "{}".format(
        #     r"sub_category_ordered_details")).mode(
        #     "overwrite").options(header=True).save()

    def hive_table_for_sold_categorywise(self):
        pass
        sqlContext = HiveContext(self.spark.sparkContext)
        sqlContext.sql('DROP TABLE IF EXISTS category')
        self.category.write.option("mode", "overwrite").saveAsTable('category')
        self.spark.sql("select count(*) from sold_category_wise").show()

    def hive_table_for_cat_productwise(self):
        pass
        sqlContext = HiveContext(self.spark.sparkContext)
        sqlContext.sql('DROP TABLE IF EXISTS product')
        self.products.write.option("mode", "overwrite").saveAsTable('product')
        self.spark.sql("select count(*) from hive_Top_ten_subcategory").show()






if __name__ == '__main__':
    # c = curated_layer()
    # c.Discount_present_y_N()
    # c.Drop_Discount()
    # c.salesamount_Freight()
    # c.agg_cat()
    # c.categorywise_agg()
    # c.curated_layer_data()
    # c.sold_category_wise_SAVE()
    # c.top_ten_subcategory_wise()
    # c.hive_table_for_sold_category_wise_product()
    # c.hive_table_for_cat_subcatagorywise()

    # SetUp
    setup = Setup()
    try:
        setup.read_from_local_clean()
    except Exception as e:
        logging.error('Error at %s', 'Reading from local', exc_info=e)
        sys.exit(1)

    # Clean
    try:
        cleanLayer = curated_layer()
    except Exception as e:
        logging.error('Error at %s', 'Error at CuratedLayer Object Creation', exc_info=e)
        sys.exit(1)

    try:
        cleanLayer.Discount_present_y_N()
    except Exception as e:
        logging.error('Error at %s', 'Error at Discount_present_y_N', exc_info=e)
        sys.exit(1)



    try:
        cleanLayer.curated()
    except Exception as e:
        logging.error('Error at %s', 'Error at salesamount_Freight', exc_info=e)
        sys.exit(1)

    try:
        cleanLayer.agg_cat()
    except Exception as e:
        logging.error('Error at %s', 'Error at agg_cat', exc_info=e)
        sys.exit(1)

    try:
        cleanLayer.products()
    except Exception as e:
        logging.error('Error at %s', 'Error at products', exc_info=e)
        sys.exit(1)

    try:
        cleanLayer.curated_layer_data()
    except Exception as e:
        logging.error('Error at %s', 'Error at write_to_curatedlayerdata', exc_info=e)
        sys.exit(1)

    try:
        cleanLayer.write_for_category()
    except Exception as e:
        logging.error('Error at %s', 'Error at sold_category_wise_SAVE', exc_info=e)
        sys.exit(1)

    try:
        cleanLayer.write_for_product()
    except Exception as e:
        logging.error('Error at %s', 'Error at write_for_product', exc_info=e)
        sys.exit(1)

    try:
        cleanLayer.hive_table_for_sold_categorywise()
    except Exception as e:
        logging.error('Error at %s', 'Error at hive_table_for_sold_category_wise_product', exc_info=e)
        sys.exit(1)

    try:
        cleanLayer.hive_table_for_cat_productwise()
    except Exception as e:
        logging.error('Error at %s', 'Error at hive_table_for_cat_productwise', exc_info=e)
        sys.exit(1)