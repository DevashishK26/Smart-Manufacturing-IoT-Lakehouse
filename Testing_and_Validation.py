# Databricks notebook source
# MAGIC %md
# MAGIC ## Row Count

# COMMAND ----------

# DBTITLE 1,iot_telemetry
import warnings

source_path = "/Volumes/sparkwars/bronze/bronze_volume/iot_telemetry/"

source_count = spark.read.json(source_path).count()

bronze_count = spark.table(
    "sparkwars.bronze.iot_telemetry_raw"
).count()

print(f"Source record count : {source_count}")
print(f"Bronze record count : {bronze_count}")

if bronze_count < source_count:
    warnings.warn(
        f"Data validation warning: Bronze layer has fewer records "
        f"({bronze_count}) than source ({source_count}). "
        f"Possible data loss during ingestion.",
        UserWarning
    )
else:
    print("Validation passed: No record loss detected in Bronze layer.")

# COMMAND ----------

# DBTITLE 1,equipment_master_raw
import warnings

source_path = "/Volumes/sparkwars/bronze/bronze_volume/equipment_master/"

source_count = (
    spark.read.format("delta")
    .load(source_path)
    .count()
)

bronze_count = spark.table("sparkwars.bronze.equipment_master_raw").count()

print(f"Source record count : {source_count}")
print(f"Bronze record count : {bronze_count}")

if bronze_count < source_count:
    warnings.warn(
        f"[DATA QUALITY WARNING] Bronze layer has fewer records "
        f"({bronze_count}) than source ({source_count}). "
        f"Possible data loss during ingestion.",
        UserWarning
    )
else:
    print("Validation passed: No record loss detected in Bronze layer.")

# COMMAND ----------

# DBTITLE 1,maintenance_records_raw
import warnings

source_count = spark.read.json(
    "/Volumes/sparkwars/bronze/bronze_volume/maintenance_records/"
).count()

bronze_count = spark.table(
    "sparkwars.bronze.maintenance_records_raw"
).count()

print(f"Source record count  : {source_count}")
print(f"Bronze record count  : {bronze_count}")

if bronze_count < source_count:
    warning_msg = (
        f"⚠ WARNING: Bronze layer has fewer records "
        f"({bronze_count}) than source ({source_count}). "
        f"Possible data loss during ingestion."
    )
    warnings.warn(warning_msg)
    print(warning_msg)
else:
    print("✅ Validation passed: No record loss detected in Bronze layer.")

# COMMAND ----------

# DBTITLE 1,production_orders_raw
import warnings

source_count = spark.read.json(
    "/Volumes/sparkwars/bronze/bronze_volume/production_orders/"
).count()

bronze_count = spark.table(
    "sparkwars.bronze.production_orders_raw"
).count()

print(f"Source record count  : {source_count}")
print(f"Bronze record count  : {bronze_count}")

if bronze_count < source_count:
    warning_msg = (
        f"⚠ WARNING: Bronze layer has fewer records "
        f"({bronze_count}) than source ({source_count}). "
        f"Possible data loss during ingestion."
    )
    warnings.warn(warning_msg)
    print(warning_msg)
else:
    print("✅ Validation passed: No record loss detected in Bronze layer.")

# COMMAND ----------

# DBTITLE 1,quality_inspection_raw
import warnings

source_count = spark.read.json(
    "/Volumes/sparkwars/bronze/bronze_volume/quality_inspection/"
).count()

bronze_count = spark.table(
    "sparkwars.bronze.quality_inspection_raw"
).count()

print(f"Source record count  : {source_count}")
print(f"Bronze record count  : {bronze_count}")

if bronze_count < source_count:
    warning_msg = (
        f"⚠ WARNING: Bronze layer has fewer records "
        f"({bronze_count}) than source ({source_count}). "
        f"Possible data loss during ingestion."
    )
    warnings.warn(warning_msg)
    print(warning_msg)
else:
    print("✅ Validation passed: No record loss detected in Bronze layer.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Validation

# COMMAND ----------

table_name = "sparkwars.bronze.iot_telemetry_raw"

expected_cols = {"device_id", "facility_id", "event_timestamp", "temperature"}
bronze_cols = set(spark.table(table_name).columns)

missing_cols = expected_cols - bronze_cols
unexpected_cols = bronze_cols - expected_cols

print(f"Validating schema for: {table_name}")
print(f"Expected columns : {sorted(expected_cols)}")
print(f"Actual columns   : {sorted(bronze_cols)}")

if missing_cols:
    raise ValueError(
        f"Schema validation failed for {table_name}. "
        f"Missing columns: {sorted(missing_cols)}"
    )

print("Schema validation passed. Required columns are present.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Metadata Columns Exist

# COMMAND ----------

table_name = "sparkwars.bronze.iot_telemetry_raw"

bronze_cols = set(spark.table(table_name).columns)
meta_cols = {"_ingestion_timestamp", "_source_file"}

missing_meta_cols = meta_cols - bronze_cols

print(f"Validating metadata columns for: {table_name}")
print(f"Required metadata columns : {sorted(meta_cols)}")
print(f"Available columns          : {sorted(bronze_cols)}")

if missing_meta_cols:
    raise ValueError(
        f"Metadata validation failed for {table_name}. "
        f"Missing metadata columns: {sorted(missing_meta_cols)}"
    )

print("Metadata validation passed. All required metadata columns are present.")

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver DQ Tests

# COMMAND ----------

table_name = "sparkwars.silver.iot_telemetry"
column_name = "device_id"

null_count = (
    spark.table(table_name)
    .filter(f"{column_name} IS NULL")
    .count()
)

print(f"Validating NOT NULL constraint on {column_name} in {table_name}")
print(f"Null count: {null_count}")

if null_count > 0:
    raise ValueError(
        f"Data quality failure: {null_count} NULL values found in "
        f"{column_name} column of {table_name}."
    )

print("Validation passed: No NULL values detected.")

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Referential Integrity Tests

# COMMAND ----------

from pyspark.sql.functions import col

fact_table = "sparkwars.gold.fact_sensor_readings"
dim_table = "sparkwars.gold.dim_equipment"

orphans_count = (
    spark.table(fact_table)
    .join(
        spark.table(dim_table),
        on="equipment_sk",
        how="left"
    )
    .filter(col(f"{dim_table.split('.')[-1]}.equipment_sk").isNull())
    .count()
)

print(f"Validating referential integrity between:")
print(f"Fact: {fact_table}")
print(f"Dimension: {dim_table}")
print(f"Orphan record count: {orphans_count}")

if orphans_count > 0:
    raise ValueError(
        f"Referential integrity failure: {orphans_count} orphaned equipment_sk "
        f"values found in {fact_table}."
    )

print("Validation passed: No orphaned equipment_sk values found.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Timestamp Coverage

# COMMAND ----------

fact_table = "sparkwars.gold.fact_sensor_readings"
dim_table = "sparkwars.gold.dim_timestamp"

missing_dates_count = (
    spark.table(fact_table)
    .join(
        spark.table(dim_table),
        on="timestamp_sk",
        how="left_anti"   # returns only unmatched rows
    )
    .count()
)

print(f"Validating timestamp dimension mapping")
print(f"Orphan timestamp_sk count: {missing_dates_count}")

if missing_dates_count > 0:
    raise ValueError(
        f"Referential integrity failure: {missing_dates_count} records in "
        f"{fact_table} have no matching timestamp_sk in {dim_table}."
    )

print("Validation passed: All timestamp_sk values are properly mapped.")

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Security Tests (RLS & CLS Simulation)

# COMMAND ----------

# DBTITLE 1,RLS
import logging

view_name = "sparkwars.gold.vw_analyst_sensor_readings"

facility_count = (
    spark.table(view_name)
    .select("facility_sk")
    .distinct()
    .count()
)

print(f"Validating RLS enforcement on {view_name}")
print(f"Distinct facility_sk count visible: {facility_count}")

if facility_count != 1:
    logging.warning(
        f"[RLS WARNING] Expected 1 facility_sk, "
        f"but found {facility_count}. "
        f"Row-level restriction may not be applied correctly."
    )
else:
    print("RLS validation passed: Data restricted to a single facility.")

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# DBTITLE 1,CLS
view_name = "sparkwars.silver.vw_maintenance_analyst"
column_name = "maintenance_technician_id"

rows = (
    spark.table(view_name)
    .select(column_name)
    .limit(10)
    .collect()
)

if not rows:
    raise ValueError(f"No records found in {view_name} to validate masking.")

masked_values = [row[column_name] for row in rows]

print(f"Validating masking on column: {column_name}")
print(f"Sample values: {masked_values}")

# Check that all sampled values are masked
if not all("***" in str(value) for value in masked_values):
    raise ValueError(
        f"Masking validation failed: Unmasked values detected in {view_name}.{column_name}"
    )

print("Masking validation passed: Column is properly masked.")