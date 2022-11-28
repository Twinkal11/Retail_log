from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import HiveContext
from pyspark.sql import functions as F
import re

class raw_ly:
    spark = SparkSession.builder.appName("retail_raw_layer") \
        .config('spark.ui.port', '4050').config("spark.master", "local") \
        .enableHiveSupport().getOrCreate()

    def read_file(self):
        df = self.spark.read.format("csv").option("header", "true").load("C:\\Retail_log_project\\input_file\\Retail.csv")

        self.df = df.withColumn("id", monotonically_increasing_id())
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

    def save(self):
        self.dataSchema.coalesce(1).write.mode("overwrite").format("csv").save(
            "C:\\Retail_log_project\\src\\internal_files\\raw_layer_retail\\", header="True")

    def show_On_Hive(self):
        pass
        sqlContext = HiveContext(self.spark.sparkContext)
        sqlContext.sql('DROP TABLE IF EXISTS Raw_layer_retail')
        self.dataSchema.coalesce(1).write.option("mode", "overwrite").saveAsTable('Raw_layer_retail')
        self.spark.sql("select count(*) from Raw_layer_retail").show()


if __name__ == '__main__':
    raw = raw_ly()
    raw.read_file()
    raw.date_type()
    raw.set_datatype()
    raw.save()
    raw.show_On_Hive()