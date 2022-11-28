from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import HiveContext
from pyspark.sql import functions as F
import re


class clean_layer():
    spark = SparkSession.builder.appName("Project-Stage-II").config('spark.ui.port', '4050') \
        .config("spark.master", "local").enableHiveSupport().getOrCreate()
    clean_df = spark.read.csv(
        "C:\\Retail_log_project\\src\\internal_files\\raw_layer_retail\\",
        header="True")

    def drop_Freight(self):
        self.DroP_Freight = self.clean_df.drop(col('Freight'))

    def set_one(self):
        self.set_one_value_OrderQuantity = self.DroP_Freight.withColumn("OrderQuantity",
                                                                        F.when(F.isnull(F.col("OrderQuantity")),
                                                                               1).otherwise(F.col("OrderQuantity")))
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

    def sep_size(self):
        self.size = self.fill_NA.withColumn("size", split(self.fill_NA["ProductName"], ',') \
                                            .getItem(1)) \
            .withColumn("ProductName", split(self.fill_NA["ProductName"], ',') \
                        .getItem(0))

        self.size.show(truncate=False)

    def group_colur(self):
        self.group_colr = self.size.groupBy("Color").count()
        self.group_colr.show()

    def colur_distinct(self):
        self.df_colour = self.group_colr.select("Color").distinct()
        self.df_colour.show()

    def colour_list(self):
        self.df_list = self.df_colour.rdd.flatMap(lambda x: x).collect()

    def remove_colr(self):
        self.df_removed_color = self.size.withColumn('ProductName',
                                                     F.regexp_replace('ProductName', '|'.join(self.df_list), '')) \
            .withColumn('ProductName', F.regexp_replace('ProductName', '- ', ''))
        self.df_removed_color.show(20, False)

    def save(self):
        self.df_removed_color.coalesce(1).write.mode("overwrite").format("csv").save(
            "C:\\Retail_log_project\\src\\internal_files\\clean_layer_retail\\", header="True")

    def show_on_hive(self):
        pass
        sqlContext = HiveContext(self.spark.sparkContext)
        sqlContext.sql('DROP TABLE IF EXISTS Cleansed_layer_retail')
        self.df_removed_color.coalesce(1).write.option("mode", "overwrite").saveAsTable('Cleansed_layer_retail')
        self.spark.sql("select count(*) from Cleansed_layer_retail").show()


if __name__ == '__main__':
    clean_l = clean_layer()
    clean_l.drop_Freight()
    clean_l.set_one()
    clean_l.check_NA()
    clean_l.fill_NA()
    clean_l.sep_size()
    clean_l.group_colur()
    clean_l.colur_distinct()
    clean_l.colour_list()
    clean_l.remove_colr()
    clean_l.save()
    clean_l.show_on_hive()

