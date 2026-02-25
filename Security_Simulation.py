# Databricks notebook source
# MAGIC %md
# MAGIC ## Row-Level Security (RLS)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Simulate current user's facility mapping (in production: use session variables)
# MAGIC CREATE OR REPLACE FUNCTION sparkwars.gold.get_user_facility()
# MAGIC RETURNS STRING
# MAGIC RETURN CASE current_user()
# MAGIC   WHEN 'analyst_na@company.com' THEN 'FAC-NA-001'
# MAGIC   WHEN 'analyst_eu@company.com' THEN 'FAC-EU-003'
# MAGIC   WHEN 'analyst_apac@company.com' THEN 'FAC-APAC-002'
# MAGIC   ELSE 'ALL'  -- Data Engineer / Admin
# MAGIC END;
# MAGIC
# MAGIC -- RLS View for Data Analysts
# MAGIC CREATE OR REPLACE VIEW sparkwars.gold.vw_analyst_sensor_readings AS
# MAGIC SELECT * FROM sparkwars.gold.fact_sensor_readings
# MAGIC WHERE facility_sk IN (
# MAGIC   SELECT facility_sk FROM sparkwars.gold.dim_facility
# MAGIC   WHERE facility_id = sparkwars.gold.get_user_facility()
# MAGIC   OR sparkwars.gold.get_user_facility() = 'ALL'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Column-Level Security (CLS)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Masked view for analysts (PII columns masked)
# MAGIC CREATE OR REPLACE VIEW sparkwars.silver.vw_maintenance_analyst AS
# MAGIC SELECT
# MAGIC   maintenance_id,
# MAGIC   equipment_id,
# MAGIC   facility_id,
# MAGIC   maintenance_type,
# MAGIC   CONCAT(LEFT(maintenance_technician_id, 5), '***') AS maintenance_technician_id,  -- Masked
# MAGIC   start_datetime,
# MAGIC   end_datetime,
# MAGIC   downtime_minutes,
# MAGIC   failure_code,
# MAGIC   maintenance_cost_usd
# MAGIC FROM sparkwars.silver.maintenance_records;
# MAGIC
# MAGIC -- Full access view for Data Engineers (no masking)
# MAGIC CREATE OR REPLACE VIEW sparkwars.silver.vw_maintenance_engineer AS
# MAGIC SELECT * FROM sparkwars.silver.maintenance_records;