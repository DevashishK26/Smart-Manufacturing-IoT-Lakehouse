# Databricks notebook source
# MAGIC %md
# MAGIC ## Audit Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE if not EXISTS sparkwars.silver.audit_log (
# MAGIC   event_timestamp TIMESTAMP,
# MAGIC   user_identity STRING,
# MAGIC   action_type STRING,      -- READ, WRITE, SCHEMA_CHANGE, SECURITY_VIOLATION
# MAGIC   target_table STRING,
# MAGIC   target_database STRING,
# MAGIC   query_text STRING,
# MAGIC   pipeline_run_id STRING
# MAGIC ) USING DELTA;

# COMMAND ----------

# MAGIC %md
# MAGIC ## IoT Telemetry Silver

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import uuid

PIPELINE_RUN_ID = str(uuid.uuid4())

BRONZE_TABLE = "sparkwars.bronze.iot_telemetry_raw"
SILVER_TABLE = "sparkwars.silver.iot_telemetry"
DQ_TABLE = "sparkwars.silver.iot_telemetry_dq_violations"

# 1. Read Bronze
bronze_df = spark.read.table(BRONZE_TABLE)

# 2. Data Quality Rule Tagging (NULL-SAFE)
dq_df = (
    bronze_df
    .withColumn("dq_device_id_not_null", col("device_id").isNotNull())
    .withColumn("dq_event_ts_not_null", col("event_timestamp").isNotNull())
    .withColumn("dq_facility_not_null", col("facility_id").isNotNull())
    .withColumn(
        "dq_temp_range",
        col("temperature").isNotNull() & col("temperature").between(-50, 150)
    )
    .withColumn(
        "dq_vibration_range",
        col("vibration_x").isNotNull() & col("vibration_x").between(0, 100)
    )
)

# 3. Overall DQ Status
dq_df = dq_df.withColumn(
    "_dq_passed",
    col("dq_device_id_not_null") &
    col("dq_event_ts_not_null") &
    col("dq_facility_not_null") &
    col("dq_temp_range") &
    col("dq_vibration_range")
)

# 4. Violation Reason (for true DQ failures)
dq_df = dq_df.withColumn(
    "_violation_reason",
    when(~col("dq_device_id_not_null"), lit("DQ-001: device_id NULL"))
    .when(~col("dq_event_ts_not_null"), lit("DQ-002: event_timestamp NULL"))
    .when(~col("dq_facility_not_null"), lit("DQ-003: facility_id NULL"))
    .when(~col("dq_temp_range"), lit("DQ-004: temperature invalid"))
    .when(~col("dq_vibration_range"), lit("DQ-005: vibration_x invalid"))
)

# 5. Add Metadata Columns
dq_df = (
    dq_df
    .withColumn("event_date", to_date("event_timestamp"))
    .withColumn("_pipeline_run_id", lit(PIPELINE_RUN_ID))
    .withColumn("_processed_timestamp", current_timestamp())
)

# 6. Split Valid / Invalid (NULL-SAFE)
valid_base_df = dq_df.filter(col("_dq_passed") == True)

invalid_df = (
    dq_df
    .filter(col("_dq_passed") != True)
    .select(
        expr("uuid()").alias("violation_id"),
        lit("iot_telemetry").alias("source_table"),
        col("device_id").alias("record_key"),
        col("_violation_reason").alias("violated_rule_desc"),
        lit("QUARANTINE").alias("action_taken"),
        current_timestamp().alias("detected_at"),
        col("_pipeline_run_id")
    )
)

# 7. Duplicate Handling (Deterministic Ranking)
window_spec = Window.partitionBy("device_id", "event_timestamp") \
                    .orderBy(col("_processed_timestamp").desc())

ranked_df = valid_base_df.withColumn("rn", row_number().over(window_spec))

deduped_valid_df = ranked_df.filter(col("rn") == 1).drop("rn")

duplicate_df = ranked_df.filter(col("rn") > 1)

duplicate_violations_df = (
    duplicate_df
    .select(
        expr("uuid()").alias("violation_id"),
        lit("iot_telemetry").alias("source_table"),
        col("device_id").alias("record_key"),
        lit("DQ-006: Duplicate telemetry event").alias("violated_rule_desc"),
        lit("QUARANTINE").alias("action_taken"),
        current_timestamp().alias("detected_at"),
        col("_pipeline_run_id")
    )
)

# 8. Write Valid Records to Silver
(
    deduped_valid_df
    .drop(
        "dq_device_id_not_null",
        "dq_event_ts_not_null",
        "dq_facility_not_null",
        "dq_temp_range",
        "dq_vibration_range",
        "_dq_passed",
        "_violation_reason"
    )
    .write
    .format("delta")
    .mode("append")
    .partitionBy("event_date", "facility_id")
    .saveAsTable(SILVER_TABLE)
)

# 9. Write All Violations (True DQ + Duplicates)
all_violations_df = invalid_df.unionByName(duplicate_violations_df)

(
    all_violations_df
    .write
    .format("delta")
    .mode("append")
    .saveAsTable(DQ_TABLE)
)


# 10. Validation Metrics
print("Total Bronze:", bronze_df.count())
print("Valid (after dedup):", deduped_valid_df.count())
print("True DQ Failures:", invalid_df.count())
print("Duplicate Violations:", duplicate_violations_df.count())

print("✅ Enterprise-grade IoT Silver processing completed successfully")




# COMMAND ----------

current_user='Devashish Kapte'
table_name='iot_telemetry'
database='sparkwars'
query='IoT Telemetry Silver'
run_id=str(uuid.uuid4())

spark.sql(f"""
INSERT INTO sparkwars.silver.audit_log 
VALUES (current_timestamp(), '{current_user}', 'WRITE', 
        '{table_name}', '{database}', '{query}', '{run_id}')
""")


# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Production Orders

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import uuid

PIPELINE_RUN_ID = str(uuid.uuid4())

BRONZE_TABLE = "sparkwars.bronze.production_orders_raw"
SILVER_TABLE = "sparkwars.silver.production_orders"
DQ_TABLE = "sparkwars.silver.production_orders_dq_violations"

# 1. Read Bronze
bronze_df = spark.read.table(BRONZE_TABLE)

# 2. NULL-SAFE DQ Rule Tagging
dq_df = (
    bronze_df
    .withColumn("dq_order_id_not_null", col("order_id").isNotNull())
    .withColumn("dq_facility_not_null", col("facility_id").isNotNull())
    .withColumn("dq_equipment_not_null", col("equipment_id").isNotNull())
)

# 3. Overall DQ Status
dq_df = dq_df.withColumn(
    "_dq_passed",
    col("dq_order_id_not_null") &
    col("dq_facility_not_null") &
    col("dq_equipment_not_null")
)

# 4. Violation Reason (True DQ Failures)
dq_df = dq_df.withColumn(
    "_violation_reason",
    when(~col("dq_order_id_not_null"), lit("DQ-PO-001: order_id NULL"))
    .when(~col("dq_facility_not_null"), lit("DQ-PO-002: facility_id NULL"))
    .when(~col("dq_equipment_not_null"), lit("DQ-PO-003: equipment_id NULL"))
)

# 5. Metadata
dq_df = (
    dq_df
    .withColumn("_pipeline_run_id", lit(PIPELINE_RUN_ID))
    .withColumn("_processed_timestamp", current_timestamp())
)

# 6. Split Valid / Invalid (NULL-SAFE)
valid_base_df = dq_df.filter(col("_dq_passed") == True)

invalid_df = (
    dq_df
    .filter(col("_dq_passed") != True)
    .select(
        expr("uuid()").alias("violation_id"),
        lit("production_orders").alias("source_table"),
        col("order_id").alias("record_key"),
        col("_violation_reason").alias("violated_rule_desc"),
        lit("QUARANTINE").alias("action_taken"),
        current_timestamp().alias("detected_at"),
        col("_pipeline_run_id")
    )
)

# 7. Deterministic Duplicate Handling
window_spec = Window.partitionBy("order_id") \
                    .orderBy(col("_processed_timestamp").desc())

ranked_df = valid_base_df.withColumn("rn", row_number().over(window_spec))

deduped_valid_df = ranked_df.filter(col("rn") == 1).drop("rn")

duplicate_df = ranked_df.filter(col("rn") > 1)

duplicate_violations_df = (
    duplicate_df
    .select(
        expr("uuid()").alias("violation_id"),
        lit("production_orders").alias("source_table"),
        col("order_id").alias("record_key"),
        lit("DQ-PO-004: Duplicate order_id").alias("violated_rule_desc"),
        lit("QUARANTINE").alias("action_taken"),
        current_timestamp().alias("detected_at"),
        col("_pipeline_run_id")
    )
)

# 8. Write Valid to Silver
(
    deduped_valid_df
    .drop(
        "dq_order_id_not_null",
        "dq_facility_not_null",
        "dq_equipment_not_null",
        "_dq_passed",
        "_violation_reason"
    )
    .write
    .format("delta")
    .mode("append")
    .partitionBy("facility_id")
    .saveAsTable(SILVER_TABLE)
)

# 9. Write All Violations (True DQ + Duplicates)
all_violations_df = invalid_df.unionByName(duplicate_violations_df)

(
    all_violations_df
    .write
    .format("delta")
    .mode("append")
    .saveAsTable(DQ_TABLE)
)

# 10. Validation Metrics
print("Total Bronze:", bronze_df.count())
print("Valid (after dedup):", deduped_valid_df.count())
print("True DQ Failures:", invalid_df.count())
print("Duplicate Violations:", duplicate_violations_df.count())

print("✅ Enterprise-grade Production Orders Silver completed")

# COMMAND ----------

current_user='Devashish Kapte'
table_name='production_orders'
database='sparkwars'
query='Production Orders'
run_id=str(uuid.uuid4())

spark.sql(f"""
INSERT INTO sparkwars.silver.audit_log 
VALUES (current_timestamp(), '{current_user}', 'WRITE', 
        '{table_name}', '{database}', '{query}', '{run_id}')
""")


# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## CDC Processing for Maintenance Records

# COMMAND ----------

# DBTITLE 1,dq_table is not working
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import uuid

PIPELINE_RUN_ID = str(uuid.uuid4())

BRONZE_TABLE = "sparkwars.bronze.maintenance_records_raw"
SILVER_TABLE = "sparkwars.silver.maintenance_records"
DQ_TABLE = "sparkwars.silver.maintenance_records_dq_violations"

# 1. Read Bronze CDC
bronze_df = spark.read.table(BRONZE_TABLE)

# 2. NULL-SAFE DQ Rule
dq_df = (
    bronze_df
    .withColumn("dq_maintenance_id_not_null", col("maintenance_id").isNotNull())
    .withColumn("_dq_passed", col("maintenance_id").isNotNull())
    .withColumn("_pipeline_run_id", lit(PIPELINE_RUN_ID))
    .withColumn("_processed_timestamp", current_timestamp())
)

# 3. Split Valid / Invalid
valid_base_df = dq_df.filter(col("_dq_passed") == True)

invalid_df = (
    dq_df
    .filter(col("_dq_passed") != True)
    .select(
        expr("uuid()").alias("violation_id"),
        lit("maintenance_records").alias("source_table"),
        col("maintenance_id").alias("record_key"),
        lit("DQ-MR-001: maintenance_id NULL").alias("violated_rule_desc"),
        lit("QUARANTINE").alias("action_taken"),
        current_timestamp().alias("detected_at"),
        col("_pipeline_run_id")
    )
)

# 4. Deterministic CDC Ranking (Latest per maintenance_id)
cdc_window = Window.partitionBy("maintenance_id") \
                   .orderBy(col("_cdc_timestamp").desc())

ranked_df = valid_base_df.withColumn("rn", row_number().over(cdc_window))

latest_cdc_df = ranked_df.filter(col("rn") == 1).drop("rn")

duplicate_cdc_df = ranked_df.filter(col("rn") > 1)

duplicate_violations_df = (
    duplicate_cdc_df
    .select(
        expr("uuid()").alias("violation_id"),
        lit("maintenance_records").alias("source_table"),
        col("maintenance_id").alias("record_key"),
        lit("DQ-MR-002: Duplicate CDC event").alias("violated_rule_desc"),
        lit("QUARANTINE").alias("action_taken"),
        current_timestamp().alias("detected_at"),
        col("_pipeline_run_id")
    )
)

# 5. Prepare Target Table (Create if not exists)
if not spark.catalog.tableExists(SILVER_TABLE):
    (
        latest_cdc_df
        .withColumn("is_deleted", lit(False))
        .withColumn("deleted_at", lit(None).cast("timestamp"))
        .write.format("delta")
        .mode("overwrite")
        .saveAsTable(SILVER_TABLE)
    )

target = DeltaTable.forName(spark, SILVER_TABLE)

# 6. Process INSERT & UPDATE
upserts = latest_cdc_df.filter(col("_cdc_operation").isin("INSERT", "UPDATE"))

(
    target.alias("t")
    .merge(
        upserts.alias("s"),
        "t.maintenance_id = s.maintenance_id"
    )
    .whenMatchedUpdate(set={
        "equipment_id": col("s.equipment_id"),
        "facility_id": col("s.facility_id"),
        "maintenance_type": col("s.maintenance_type"),
        "downtime_minutes": col("s.downtime_minutes"),
        "maintenance_cost_usd": col("s.maintenance_cost_usd"),
        "_pipeline_run_id": col("s._pipeline_run_id")
    })
    .whenNotMatchedInsert(values={
        "maintenance_id": col("s.maintenance_id"),
        "equipment_id": col("s.equipment_id"),
        "facility_id": col("s.facility_id"),
        "maintenance_type": col("s.maintenance_type"),
        "downtime_minutes": col("s.downtime_minutes"),
        "maintenance_cost_usd": col("s.maintenance_cost_usd"),
        "is_deleted": lit(False),
        "deleted_at": lit(None).cast("timestamp"),
        "_pipeline_run_id": col("s._pipeline_run_id")
    })
    .execute()
)

# 7. Process DELETE (Soft Delete)
deletes = latest_cdc_df.filter(col("_cdc_operation") == "DELETE")

(
    target.alias("t")
    .merge(
        deletes.alias("s"),
        "t.maintenance_id = s.maintenance_id"
    )
    .whenMatchedUpdate(set={
        "is_deleted": lit(True),
        "deleted_at": current_timestamp(),
        "_pipeline_run_id": col("s._pipeline_run_id")
    })
    .execute()
)

# 8. Write All Violations (NULL + Duplicate CDC)
all_violations_df = invalid_df.unionByName(duplicate_violations_df)

(
    all_violations_df
    .write
    .format("delta")
    .mode("append")
    .saveAsTable(DQ_TABLE)
)

# 9. Validation Metrics
print("Total Bronze:", bronze_df.count())
print("Valid Latest CDC:", latest_cdc_df.count())
print("Null Violations:", invalid_df.count())
print("Duplicate CDC Violations:", duplicate_violations_df.count())

print("✅ Enterprise-grade Maintenance CDC processing completed successfully")

# COMMAND ----------

current_user='Devashish Kapte'
table_name='maintenance_records'
database='sparkwars'
query='CDC Processing for Maintenance Records'
run_id=str(uuid.uuid4())

spark.sql(f"""
INSERT INTO sparkwars.silver.audit_log 
VALUES (current_timestamp(), '{current_user}', 'WRITE', 
        '{table_name}', '{database}', '{query}', '{run_id}')
""")


# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality Inspection
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import uuid

PIPELINE_RUN_ID = str(uuid.uuid4())

BRONZE_TABLE = "sparkwars.bronze.quality_inspection_raw"
SILVER_TABLE = "sparkwars.silver.quality_inspection"
DQ_TABLE = "sparkwars.silver.quality_inspection_dq_violations"

# 1. Read Bronze
bronze_df = spark.read.table(BRONZE_TABLE)

# 2. NULL-SAFE DQ Rule Tagging
dq_df = (
    bronze_df
    .withColumn("dq_inspection_id_not_null", col("inspection_id").isNotNull())
    .withColumn("dq_order_id_not_null", col("order_id").isNotNull())
    .withColumn(
        "dq_units_valid",
        col("units_inspected").isNotNull() &
        col("units_passed").isNotNull() &
        col("units_rejected").isNotNull() &
        (col("units_inspected") >= 0) &
        (col("units_passed") >= 0) &
        (col("units_rejected") >= 0)
    )
    .withColumn(
        "dq_units_consistent",
        col("units_passed").isNotNull() &
        col("units_rejected").isNotNull() &
        col("units_inspected").isNotNull() &
        (col("units_passed") + col("units_rejected") <= col("units_inspected"))
    )
    .withColumn("dq_timestamp_not_null", col("inspection_timestamp").isNotNull())
)

# 3. Overall DQ Status
dq_df = dq_df.withColumn(
    "_dq_passed",
    col("dq_inspection_id_not_null") &
    col("dq_order_id_not_null") &
    col("dq_units_valid") &
    col("dq_units_consistent") &
    col("dq_timestamp_not_null")
)

# 4. Violation Reason (True DQ Failures)
dq_df = dq_df.withColumn(
    "_violation_reason",
    when(~col("dq_inspection_id_not_null"), lit("DQ-QI-001: inspection_id NULL"))
    .when(~col("dq_order_id_not_null"), lit("DQ-QI-002: order_id NULL"))
    .when(~col("dq_units_valid"), lit("DQ-QI-003: invalid unit values"))
    .when(~col("dq_units_consistent"), lit("DQ-QI-004: units inconsistent"))
    .when(~col("dq_timestamp_not_null"), lit("DQ-QI-005: inspection_timestamp NULL"))
)

# 5. Derived Columns
dq_df = (
    dq_df
    .withColumn("inspection_date", to_date("inspection_timestamp"))
    .withColumn(
        "yield_rate",
        when(col("units_inspected") > 0,
             col("units_passed") / col("units_inspected"))
    )
    .withColumn(
        "defect_rate",
        when(col("units_inspected") > 0,
             col("units_rejected") / col("units_inspected"))
    )
    .withColumn("_pipeline_run_id", lit(PIPELINE_RUN_ID))
    .withColumn("_processed_timestamp", current_timestamp())
)

# 6. Split Valid / Invalid (NULL-SAFE)
valid_base_df = dq_df.filter(col("_dq_passed") == True)

invalid_df = (
    dq_df
    .filter(col("_dq_passed") != True)
    .select(
        expr("uuid()").alias("violation_id"),
        lit("quality_inspection").alias("source_table"),
        col("inspection_id").alias("record_key"),
        col("_violation_reason").alias("violated_rule_desc"),
        lit("QUARANTINE").alias("action_taken"),
        current_timestamp().alias("detected_at"),
        col("_pipeline_run_id")
    )
)

# 7. Deterministic Duplicate Handling
window_spec = Window.partitionBy("inspection_id") \
                    .orderBy(col("_processed_timestamp").desc())

ranked_df = valid_base_df.withColumn("rn", row_number().over(window_spec))

deduped_valid_df = ranked_df.filter(col("rn") == 1).drop("rn")

duplicate_df = ranked_df.filter(col("rn") > 1)

duplicate_violations_df = (
    duplicate_df
    .select(
        expr("uuid()").alias("violation_id"),
        lit("quality_inspection").alias("source_table"),
        col("inspection_id").alias("record_key"),
        lit("DQ-QI-006: Duplicate inspection_id").alias("violated_rule_desc"),
        lit("QUARANTINE").alias("action_taken"),
        current_timestamp().alias("detected_at"),
        col("_pipeline_run_id")
    )
)

# 8. Write Valid to Silver
(
    deduped_valid_df
    .drop(
        "dq_inspection_id_not_null",
        "dq_order_id_not_null",
        "dq_units_valid",
        "dq_units_consistent",
        "dq_timestamp_not_null",
        "_dq_passed",
        "_violation_reason"
    )
    .write
    .format("delta")
    .mode("append")
    .partitionBy("inspection_date", "facility_id")
    .saveAsTable(SILVER_TABLE)
)

# 9. Write All Violations (True DQ + Duplicates)
all_violations_df = invalid_df.unionByName(duplicate_violations_df)

(
    all_violations_df
    .write
    .format("delta")
    .mode("append")
    .saveAsTable(DQ_TABLE)
)

# 10. Validation Metrics
print("Total Bronze:", bronze_df.count())
print("Valid (after dedup):", deduped_valid_df.count())
print("True DQ Failures:", invalid_df.count())
print("Duplicate Violations:", duplicate_violations_df.count())

print("✅ Enterprise-grade Quality Inspection Silver completed")

# COMMAND ----------

current_user='Devashish Kapte'
table_name='quality_inspection'
database='sparkwars'
query='Quality Inspection'
run_id=str(uuid.uuid4())

spark.sql(f"""
INSERT INTO sparkwars.silver.audit_log 
VALUES (current_timestamp(), '{current_user}', 'WRITE', 
        '{table_name}', '{database}', '{query}', '{run_id}')
""")


# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCD Type 2 for Equipment Master

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import uuid

PIPELINE_RUN_ID = str(uuid.uuid4())
SOURCE_TABLE = "sparkwars.bronze.equipment_master_raw"
TARGET_TABLE = "sparkwars.silver.equipment_master"

source_df = spark.read.table(SOURCE_TABLE)


# 1. Create hash for change detection
business_cols = [
    "equipment_status",
    "rated_capacity",
    "manufacturer",
    "model_number",
    "criticality_level"
]

source_df = source_df.withColumn(
    "row_hash",
    sha2(concat_ws("||", *business_cols), 256)
)


# 2. Initialize target if not exists
if not spark.catalog.tableExists(TARGET_TABLE):

    (
        source_df
        .withColumn("effective_start_date", current_date())
        .withColumn("effective_end_date", lit(None).cast("date"))
        .withColumn("is_current", lit(True))
        .withColumn("version_number", lit(1))
        .withColumn("_pipeline_run_id", lit(PIPELINE_RUN_ID))
        .write.format("delta")
        .mode("overwrite")
        .saveAsTable(TARGET_TABLE)
    )

    print("✅ Equipment master initialized")

else:

    target = DeltaTable.forName(spark, TARGET_TABLE)
    current_df = target.toDF().filter("is_current = true")
    
    # 3. Detect new equipment
    new_records = (
        source_df.alias("s")
        .join(current_df.alias("t"), "equipment_id", "left_anti")
    )

    
    # 4. Detect changed equipment
    changed_records = (
        source_df.alias("s")
        .join(current_df.alias("t"), "equipment_id")
        .filter("s.row_hash <> t.row_hash")
        .select("s.*", "t.version_number")
    )

    
    # 5. Expire old versions
    if changed_records.limit(1).count() > 0:

        (
            target.alias("t")
            .merge(
                changed_records.alias("s"),
                "t.equipment_id = s.equipment_id AND t.is_current = true"
            )
            .whenMatchedUpdate(set={
                "is_current": lit(False),
                "effective_end_date": current_date()
            })
            .execute()
        )

    
    # 6. Insert new versions (for changed)
    new_versions = (
        changed_records
        .withColumn("version_number", col("version_number") + 1)
        .withColumn("effective_start_date", current_date())
        .withColumn("effective_end_date", lit(None).cast("date"))
        .withColumn("is_current", lit(True))
        .withColumn("_pipeline_run_id", lit(PIPELINE_RUN_ID))
        .drop("row_hash")
    )

    
    # 7. Insert brand new equipment
    new_records_insert = (
        new_records
        .withColumn("version_number", lit(1))
        .withColumn("effective_start_date", current_date())
        .withColumn("effective_end_date", lit(None).cast("date"))
        .withColumn("is_current", lit(True))
        .withColumn("_pipeline_run_id", lit(PIPELINE_RUN_ID))
        .drop("row_hash")
    )

    
    # 8. Append inserts
    final_insert_df = new_versions.unionByName(new_records_insert)

    if final_insert_df.limit(1).count() > 0:
        final_insert_df.write.format("delta").mode("append").saveAsTable(TARGET_TABLE)

    print("✅ Equipment SCD2 processing completed")

# COMMAND ----------

current_user='Devashish Kapte'
table_name='equipment_master'
database='sparkwars'
query='SCD Type 2 for Equipment Master'
run_id=str(uuid.uuid4())

spark.sql(f"""
INSERT INTO sparkwars.silver.audit_log 
VALUES (current_timestamp(), '{current_user}', 'WRITE', 
        '{table_name}', '{database}', '{query}', '{run_id}')
""")


# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC