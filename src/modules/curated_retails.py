from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import HiveContext
from pyspark.sql import functions as F
import re
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number


class curated_layer():
    spark = SparkSession.builder.appName("Project-Stage-II").config('spark.ui.port', '4050') \
        .config("spark.master", "local").enableHiveSupport().getOrCreate()
    curated = spark.read.csv("C:\\Retail_log_project\\src\\internal_files\\clean_layer_retail\\", header="True")

    def Discount_present_y_N(self):
        self.discount_present = self.curated.withColumn('DiscountAmount_Y/N', when(col('DiscountAmount') == 0.0, "NO") \
                                                        .otherwise('YES'))
        self.discount_present.show()

    def Drop_Discount(self):
        self.remove_discount = self.discount_present.drop(col('DiscountAmount'))
        #
        # self.remove_discount.show()

    def salesamount_Freight(self):
        self.curated_l = self.remove_discount.withColumn("salesprice-freight-taxes-result",
                                                         F.col("SalesAmount") - (F.col("TaxAmount") + F.col("Freight")))
        # self.curated_l.show()

    def agg_cat(self):
        self.agg_OrderQuantity_SalesAmount = self.remove_discount.groupBy("Category").agg(
            F.sum("OrderQuantity").alias("OrderQuantity"), F.sum("SalesAmount").alias("SalesAmount")).orderBy(
            F.col("OrderQuantity").desc())
        self.agg_OrderQuantity_SalesAmount.show()

    def categorywise_agg(self):
        self.categorywise_agg = self.remove_discount.groupBy("Category", "Subcategory").agg(
            F.sum("OrderQuantity").alias("OrderQuantity"),
            F.sum("SalesAmount").alias("SalesAmount")) \
            .orderBy(F.col("OrderQuantity").asc())
        cat_sub = Window.partitionBy("Category").orderBy(F.col("OrderQuantity").desc())
        self.curated_df = self.categorywise_agg.withColumn("row", F.row_number().over(cat_sub)) \
            .filter(F.col("row") <= 10).drop("row")

        self.curated_df.show()

    def curated_layer_data(self):
        self.curated_l.coalesce(1).write.mode("overwrite").format('csv').option("header", True).save(
            "C:\\Retail_log_project\\src\\internal_files\\curated_layer_Retail_result\\curated_layer_Retail_result\\curated_layer_Retail_result.csv")


    def sold_category_wise_SAVE(self):
        self.agg_OrderQuantity_SalesAmount.coalesce(1).write.mode("overwrite").format('csv').option("header", True).save(
            "C:\\Retail_log_project\\src\\internal_files\\curated_layer_Retail_result\\agg_orderquantity_SalesAmount\\agg_orderquantity_SalesAmount.csv")

    def sales_per_region_wise(self):
        self.curated_df.coalesce(1).write.mode("overwrite").format('csv').option("header", True).save(
            "C:\\Retail_log_project\\src\\internal_files\\curated_layer_Retail_result\\Top_ten_subcategory\\Top_ten_subcategory.csv")


    def hive_table_for_sold_category_wise_product(self):
        pass
        sqlContext = HiveContext(self.spark.sparkContext)
        sqlContext.sql('DROP TABLE IF EXISTS hive_sold_category_wise')
        self.agg_OrderQuantity_SalesAmount.write.option("mode", "overwrite").saveAsTable('hive_sold_category_wise')
        self.spark.sql("select count(*) from sold_category_wise").show()

    def hive_table_for_cat_subcatagorywise(self):
        pass
        sqlContext = HiveContext(self.spark.sparkContext)
        sqlContext.sql('DROP TABLE IF EXISTS hive_Top_ten_subcategory')
        self.curated_df.write.option("mode", "overwrite").saveAsTable('hive_Top_ten_subcategory')
        self.spark.sql("select count(*) from hive_Top_ten_subcategory").show()


if __name__ == '__main__':
    c = curated_layer()
    c.Discount_present_y_N()
    c.Drop_Discount()
    c.salesamount_Freight()
    c.agg_cat()
    c.categorywise_agg()
    c.curated_layer_data()
    c.sold_category_wise_SAVE()
    c.sales_per_region_wise()
    c.hive_table_for_sold_category_wise_product()
    c.hive_table_for_cat_subcatagorywise()

