from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import HiveContext
from pyspark.sql import functions as F
import re


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

        self.remove_discount.show()

    # Aggregations:
    # Check how many products have been sold under each category.
    # Check how much sales is done in each region

    def sold_categoryWise(self):
        self.Product_sold = self.remove_discount.groupBy("Category") \
            .agg(sum("OrderQuantity").alias("OrderQuantity"))

        self.Product_sold.show()

    def sales_per_region(self):
        self.sales_per_region = self.remove_discount.groupBy("SalesRegion") \
            .agg(sum("OrderQuantity").alias("OrderQuantity"))

        self.sales_per_region.show()

    def curated_layer_data(self):
        self.remove_discount.write.csv(
            "C:\\Retail_log_project\\src\\internal_files\\curated_layer_Retail_result\\",
            header=True, mode='overwrite')

    def sold_category_wise_SAVE(self):
        self.Product_sold.write.csv(
            "C:\\Retail_log_project\\src\\internal_files\\curated_layer_Retail_result\\catagorywise_sold\\", header=True)

    def sales_per_region_wise(self):
        self.sales_per_region.write.csv(
            "C:\\Retail_log_project\\src\\internal_files\\curated_layer_Retail_result\\sales_per_region\\", header=True)

    def hive_table_for_sold_category_wise_product(self):
        pass
        sqlContext = HiveContext(self.spark.sparkContext)
        sqlContext.sql('DROP TABLE IF EXISTS sold_category_wise')
        self.Product_sold.write.option("mode", "overwrite").saveAsTable('sold_category_wise')
        self.spark.sql("select count(*) from sold_category_wise").show()

    def hive_table_for_sales_per_region_wise(self):
        pass
        sqlContext = HiveContext(self.spark.sparkContext)
        sqlContext.sql('DROP TABLE IF EXISTS sales_per_region_wise')
        self.sales_per_region.write.option("mode", "overwrite").saveAsTable('sales_per_region_wise')
        self.spark.sql("select count(*) from sales_per_region_wise").show()


if __name__ == '__main__':
    curated = curated_layer()
    curated.Discount_present_y_N()
    curated.Drop_Discount()
    curated.sold_categoryWise()
    curated.sales_per_region()
    curated.curated_layer_data()
    curated.sold_category_wise_SAVE()
    curated.sales_per_region_wise()
    curated.hive_table_for_sold_category_wise_product()
    curated.hive_table_for_sales_per_region_wise()