from pyspark.sql import *

def write_to_snowflake():
    SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
    snowflake_database = "TWINKAL_RETAIL"
    snowflake_schema = "public"
    target_table_name = "curated_log_details"
    snowflake_options = {
        "sfUrl": "jn94146.ap-south-1.aws.snowflakecomputing.com",
        "sfUser": "sushantsangle",
        "sfPassword": "Stanford@01",
        "sfDatabase": snowflake_database,
        "sfSchema": snowflake_schema,
        "sfWarehouse": "curated_snowflake"
    }
    spark = SparkSession.builder \
        .appName("Demo_Project").enableHiveSupport().getOrCreate()
    df_raw = spark.read.format("csv").option("header","True").load(
        "s3://twinkal-db//Raw_layer_retail.csv")
    raw = df_raw.select("*")
    raw.write.format("snowflake")\
        .options(**snowflake_options) \
        .option("dbtable", "Raw_layer_retail") \
        .option("header", "true") \
        .mode("overwrite") \
        .save()
    df_cleans = spark.read.format("csv").option("header","True").load(
        "s3://twinkal-db//clean_layer_retail.csv")
    cleans = df_cleans.select("*")
    cleans.write.format("snowflake") \
        .options(**snowflake_options) \
        .option("dbtable", "clean_layer_retail") \
        .option("header", "true") \
        .mode("overwrite") \
        .save()
    df_curate = spark.read.format("csv").option("header","True").load(
        "s3://twinkal-db//curated_layer_retail.csv")
    curate = df_curate.select("*")
    curate.write.format("snowflake") \
        .options(**snowflake_options) \
        .option("dbtable", "curated_layer_retail") \
        .option("header", "true") \
        .mode("overwrite") \
        .save()
    df_per = spark.read.format("csv").option("header","True").load(
        "s3://twinkal-db//curated_sales_per_Region.csv")
    per = df_per.select("*")
    per.write.format("snowflake") \
        .options(**snowflake_options) \
        .option("dbtable", "curated_sales_per_Region") \
        .option("header", "true") \
        .mode("overwrite") \
        .save()
    df_across = spark.read.format("csv").option("header","True").load(
        "s3://twinkal-db//curated_categary_wise_sold.csv")
    across = df_across.select("*")
    across.write.format("snowflake") \
        .options(**snowflake_options) \
        .option("dbtable", "curated_categary_wise_sold") \
        .option("header", "true") \
        .mode("overwrite") \
        .save()

write_to_snowflake()