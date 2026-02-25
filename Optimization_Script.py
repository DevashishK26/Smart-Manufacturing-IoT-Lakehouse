# Databricks notebook source
# MAGIC %md
# MAGIC ## Z-Order Script

# COMMAND ----------

# SILVER
spark.sql("OPTIMIZE sparkwars.silver.iot_telemetry ZORDER BY (device_id, event_timestamp)")
spark.sql("OPTIMIZE sparkwars.silver.quality_inspection ZORDER BY (order_id, equipment_id)")
spark.sql("OPTIMIZE sparkwars.silver.maintenance_records ZORDER BY (equipment_id, maintenance_type, start_datetime)")

# GOLD
spark.sql("OPTIMIZE sparkwars.gold.dim_timestamp")
spark.sql("OPTIMIZE sparkwars.gold.dim_production_order ZORDER BY (order_id, facility_id)")
spark.sql("OPTIMIZE sparkwars.gold.fact_sensor_readings ZORDER BY (equipment_sk)")

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auto-Compaction & Optimize Writes

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC
# MAGIC -- Set on all fact tables
# MAGIC ALTER TABLE sparkwars.silver.iot_telemetry
# MAGIC SET TBLPROPERTIES (
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC   'delta.autoOptimize.autoCompact' = 'true',
# MAGIC   'delta.targetFileSize' = '134217728'  -- 128MB target
# MAGIC );
# MAGIC
# MAGIC ALTER TABLE sparkwars.gold.fact_sensor_readings
# MAGIC SET TBLPROPERTIES (
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC   'delta.autoOptimize.autoCompact' = 'true',
# MAGIC   'delta.targetFileSize' = '134217728'  -- 128MB target
# MAGIC );
# MAGIC
# MAGIC
# MAGIC ALTER TABLE sparkwars.silver.maintenance_records 
# MAGIC SET TBLPROPERTIES (
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC   'delta.autoOptimize.autoCompact' = 'true',
# MAGIC   'delta.targetFileSize' = '134217728'  -- 128MB target
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## VACUUM Policy

# COMMAND ----------

# Retention: 7 days for production (168 hours)
spark.sql("VACUUM sparkwars.silver.iot_telemetry RETAIN 168 HOURS")
spark.sql("VACUUM sparkwars.gold.fact_sensor_readings RETAIN 168 HOURS")
spark.sql("VACUUM sparkwars.silver.maintenance_records  RETAIN 168 HOURS")

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC