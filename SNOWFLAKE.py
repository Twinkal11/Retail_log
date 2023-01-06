from pyspark.sql import *

def write_to_snowflake():
    SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
    snowflake_database = "TWINKAL_RETAIL"
    snowflake_schema = "public"
    target_table_name = "curated_log_details"
    snowflake_options = {
        "sfUrl": "cz67521.ap-southeast-1.snowflakecomputing.com",
        "sfUser": "twinkal",
        "sfPassword": "Twinkal@123",
        "sfDatabase": snowflake_database,
        "sfSchema": snowflake_schema,
        "sfWarehouse": "COMPUTE_DATA"
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
        "s3://twinkal-db/Clean_layer_retail.csv")
    cleans = df_cleans.select("*")
    cleans.write.format("snowflake") \
        .options(**snowflake_options) \
        .option("dbtable", "clean_layer_retail") \
        .option("header", "true") \
        .mode("overwrite") \
        .save()
    df_curate = spark.read.format("csv").option("header","True").load(
        "s3://twinkal-db//Curated_layer_Retail.csv")
    curate = df_curate.select("*")
    curate.write.format("snowflake") \
        .options(**snowflake_options) \
        .option("dbtable", "curated_layer_retail") \
        .option("header", "true") \
        .mode("overwrite") \
        .save()
    df_per = spark.read.format("csv").option("header","True").load(
        "s3://twinkal-db//category.csv")
    per = df_per.select("*")
    per.write.format("snowflake") \
        .options(**snowflake_options) \
        .option("dbtable", "category") \
        .option("header", "true") \
        .mode("overwrite") \
        .save()
    df_across = spark.read.format("csv").option("header","True").load(
        "s3://twinkal-db//product.csv")
    across = df_across.select("*")
    across.write.format("snowflake") \
        .options(**snowflake_options) \
        .option("dbtable", "product") \
        .option("header", "true") \
        .mode("overwrite") \
        .save()

write_to_snowflake()
