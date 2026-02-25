# Databricks notebook source
# MAGIC %md
# MAGIC ## IoT Telemetry 
# MAGIC Streaming simulation

# COMMAND ----------

# IOT_LANDING_PATH = "/Volumes/sparkwars/bronze/bronze_volume/iot_telemetry/"

# COMMAND ----------

# DBTITLE 1,delete
# sample_df = spark.read.format("json").load(IOT_LANDING_PATH)
# schema = sample_df.schema

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col

IOT_LANDING_PATH = "/Volumes/sparkwars/bronze/bronze_volume/iot_telemetry/"

sample_df = spark.read.format("json").load(IOT_LANDING_PATH)
schema = sample_df.schema

iot_stream = (
    spark.readStream
        .format("json")
        .schema(schema)
        .load(IOT_LANDING_PATH)
)

iot_bronze = (
    iot_stream
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
)

(
    iot_bronze.writeStream
        .format("delta")
        .outputMode("append")
        .trigger(availableNow=True)
        .option("checkpointLocation", "/Volumes/sparkwars/bronze/bronze_volume/checkpoints/iot/")
        .toTable("sparkwars.bronze.iot_telemetry_raw")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE sparkwars.bronze.iot_telemetry_raw
# MAGIC SET TBLPROPERTIES (
# MAGIC   'delta.enableChangeDataFeed' = 'false',
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC   'delta.autoOptimize.autoCompact' = 'true',
# MAGIC   'comment' = 'Raw IoT telemetry — append only, no transformations'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Production Orders 
# MAGIC Batch with Autoloader simulation

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col

# 1. Setup paths
PROD_PATH = "/Volumes/sparkwars/bronze/bronze_volume/production_orders/"
CHECKPOINT_PATH = "/Volumes/sparkwars/bronze/bronze_volume/checkpoints/production_orders/"

# 2. Auto Loader stream
prod_stream = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", CHECKPOINT_PATH)
        .option("rescuedDataColumn", "_rescued_data")
        .load(PROD_PATH)
)

# 3. Add metadata (Unity Catalog compliant)
prod_bronze = (
    prod_stream
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
)

# 4. Write to Delta
(
    prod_bronze.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(availableNow=True)
        .toTable("sparkwars.bronze.production_orders_raw")
)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Maintenance Records
# MAGIC CDC Parquet

# COMMAND ----------

MAINT_PATH = "/Volumes/sparkwars/bronze/bronze_volume/maintenance_records/"

maint_df = spark.read.parquet(MAINT_PATH)

maint_bronze = (
    maint_df
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
)

maint_bronze.write.mode("append").format("delta") \
    .saveAsTable("sparkwars.bronze.maintenance_records_raw")

# COMMAND ----------

# DBTITLE 1,Detailed meta data
# from pyspark.sql.functions import current_timestamp, col

# MAINT_PATH = "/Volumes/sparkwars/bronze/bronze_volume/maintenance_records/"

# maint_df = spark.read.parquet(MAINT_PATH)

# maint_bronze = (
#     maint_df
#         .withColumn("_ingestion_timestamp", current_timestamp())
#         .withColumn("_source_file_path", col("_metadata.file_path"))
#         .withColumn("_source_file_name", col("_metadata.file_name"))
#         .withColumn("_source_file_size", col("_metadata.file_size"))
#         .withColumn("_source_file_modification_time", col("_metadata.file_modification_time"))
# )

# maint_bronze.write.mode("append") \
#     .format("delta") \
#     .saveAsTable("sparkwars.bronze.maintenance_records_raw_delete")

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality Inspection

# COMMAND ----------

QUALITY_PATH = "/Volumes/sparkwars/bronze/bronze_volume/quality_inspection/"

quality_df = (
    spark.read
        .format("csv")
        .option("header", True)
        .option("inferSchema", True)
        .load(QUALITY_PATH)
)

quality_bronze = (
    quality_df
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
)

quality_bronze.write.mode("append").format("delta") \
    .saveAsTable("sparkwars.bronze.quality_inspection_raw")

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Equipment Master
# MAGIC Delta Source Copy

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

EQUIP_PATH = "/Volumes/sparkwars/bronze/bronze_volume/equipment_master/"

equip_df = spark.read.format("delta").load(EQUIP_PATH)

equip_bronze = (
    equip_df
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", lit("equipment_master_delta"))
)

equip_bronze.write.mode("append").format("delta") \
    .saveAsTable("sparkwars.bronze.equipment_master_raw")