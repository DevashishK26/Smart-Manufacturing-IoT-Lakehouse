# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import uuid

PIPELINE_RUN_ID = str(uuid.uuid4())

SILVER_SCHEMA = "sparkwars.silver"
MASTER_DQ_TABLE = "sparkwars.silver.dq_violations_master"

dq_tables = [
    "sparkwars.silver.iot_telemetry_dq_violations",
    "sparkwars.silver.production_orders_dq_violations",
    "sparkwars.silver.quality_inspection_dq_violations",
    "sparkwars.silver.maintenance_records_dq_violations"
]

dq_dfs = []

for table in dq_tables:
    if spark.catalog.tableExists(table):
        dq_dfs.append(spark.read.table(table))

if dq_dfs:
    unified_dq = dq_dfs[0]
    for df in dq_dfs[1:]:
        unified_dq = unified_dq.unionByName(df, allowMissingColumns=True)
else:
    unified_dq = None

quality_df = spark.read.table("sparkwars.silver.quality_inspection")
production_df = spark.read.table("sparkwars.silver.production_orders")

invalid_fk = (
    quality_df.alias("q")
    .join(production_df.alias("p"), "order_id", "left_anti")
    .select(
        expr("uuid()").alias("violation_id"),
        lit("quality_inspection").alias("source_table"),
        col("inspection_id").alias("record_key"),
        lit("DQ-010").alias("violated_rule_id"),
        lit("order_id does not exist in production_orders").alias("violated_rule_desc"),
        lit("order_id").alias("field_name"),
        col("order_id").cast("string").alias("field_value"),
        lit("WARN").alias("action_taken"),
        current_timestamp().alias("detected_at"),
        lit(PIPELINE_RUN_ID).alias("pipeline_run_id")
    )
)



quality_df = spark.read.table("sparkwars.silver.quality_inspection")
production_df = spark.read.table("sparkwars.silver.production_orders")

invalid_fk = (
    quality_df.alias("q")
    .join(production_df.alias("p"), "order_id", "left_anti")
    .select(
        expr("uuid()").alias("violation_id"),
        lit("quality_inspection").alias("source_table"),
        col("inspection_id").alias("record_key"),
        lit("DQ-010").alias("violated_rule_id"),
        lit("order_id does not exist in production_orders").alias("violated_rule_desc"),
        lit("order_id").alias("field_name"),
        col("order_id").cast("string").alias("field_value"),
        lit("WARN").alias("action_taken"),
        current_timestamp().alias("detected_at"),
        lit(PIPELINE_RUN_ID).alias("pipeline_run_id")
    )
)


all_cross_checks = invalid_fk

if unified_dq:
    final_dq = unified_dq.unionByName(all_cross_checks, allowMissingColumns=True)
else:
    final_dq = all_cross_checks



final_dq.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(MASTER_DQ_TABLE)

print("✅ Master DQ table updated successfully")

# COMMAND ----------

print("IoT DQ:", spark.read.table("sparkwars.silver.iot_telemetry_dq_violations").count())
print("Production DQ:", spark.read.table("sparkwars.silver.production_orders_dq_violations").count())
print("Quality DQ:", spark.read.table("sparkwars.silver.quality_inspection_dq_violations").count())
print("Maintenance DQ:", spark.read.table("sparkwars.silver.maintenance_records_dq_violations").count())

print("FK Violations:", invalid_fk.count())
# print("Stale Records:", stale_records.count())

# COMMAND ----------

print("Final DQ:", final_dq.count())