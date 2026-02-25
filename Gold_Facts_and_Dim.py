# Databricks notebook source
# MAGIC %md
# MAGIC ## DDL for Dimension Tables

# COMMAND ----------

# DBTITLE 1,dim_facility
# MAGIC %sql
# MAGIC CREATE or replace TABLE sparkwars.gold.dim_facility (
# MAGIC   facility_sk BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   facility_id STRING,
# MAGIC   facility_name STRING,
# MAGIC   region STRING,
# MAGIC   country STRING,
# MAGIC   city STRING,
# MAGIC   timezone STRING,
# MAGIC   facility_status STRING,
# MAGIC   is_active BOOLEAN,
# MAGIC   created_at TIMESTAMP,
# MAGIC   updated_at TIMESTAMP
# MAGIC ) USING DELTA;

# COMMAND ----------

# DBTITLE 1,dim_timestamp
from pyspark.sql.functions import (
    explode, sequence, to_date, year, month, quarter,
    weekofyear, dayofweek, date_format, dayofmonth,
    when, col, last_day, current_timestamp
)

# Generate date range
date_range = spark.sql("""
    SELECT explode(
        sequence(to_date('2025-01-01'),
                 to_date('2027-12-31'),
                 interval 1 day)
    ) AS date_day
""")

dim_ts = (
    date_range
    .withColumn("timestamp_sk", date_format("date_day", "yyyyMMdd").cast("int"))
    .withColumn("year", year("date_day"))
    .withColumn("quarter", quarter("date_day"))
    .withColumn("month", month("date_day"))
    .withColumn("month_name", date_format("date_day", "MMMM"))
    .withColumn("month_short_name", date_format("date_day", "MMM"))
    .withColumn("week_of_year", weekofyear("date_day"))
    .withColumn("day_of_month", dayofmonth("date_day"))
    .withColumn("day_of_week_number", dayofweek("date_day"))
    .withColumn("day_name", date_format("date_day", "EEEE"))
    .withColumn("is_weekend", when(dayofweek("date_day").isin(1,7), True).otherwise(False))
    .withColumn("is_month_end", col("date_day") == last_day("date_day"))
    .withColumn("is_year_end",
                (month("date_day") == 12) & (dayofmonth("date_day") == 31))
    .withColumn("fiscal_quarter", 
                when(month("date_day").between(1,3), "FQ1")
                .when(month("date_day").between(4,6), "FQ2")
                .when(month("date_day").between(7,9), "FQ3")
                .otherwise("FQ4"))
    .withColumn("created_at", current_timestamp())
)

# Write to Gold
dim_ts.write.format("delta").mode("overwrite").saveAsTable("sparkwars.gold.dim_timestamp")

# COMMAND ----------

# DBTITLE 1,dim_technician
# MAGIC %sql
# MAGIC CREATE or replace TABLE sparkwars.gold.dim_technician (
# MAGIC   technician_sk BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   technician_id STRING,
# MAGIC   technician_name STRING,
# MAGIC   certification_level STRING,
# MAGIC   region STRING,
# MAGIC   is_active BOOLEAN,
# MAGIC   created_at TIMESTAMP
# MAGIC ) USING DELTA;

# COMMAND ----------

# DBTITLE 1,dim_production_order
# MAGIC %sql
# MAGIC CREATE or replace TABLE sparkwars.gold.dim_production_order (
# MAGIC   order_sk BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   order_id STRING,
# MAGIC   product_type STRING,
# MAGIC   priority STRING,
# MAGIC   planned_start_date DATE,
# MAGIC   planned_end_date DATE,
# MAGIC   facility_id STRING,
# MAGIC   created_at TIMESTAMP
# MAGIC ) USING DELTA;

# COMMAND ----------

# DBTITLE 1,dim_equipment
# MAGIC %sql
# MAGIC CREATE or replace TABLE sparkwars.gold.dim_equipment (
# MAGIC   equipment_sk BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   equipment_id STRING,
# MAGIC   facility_id STRING,
# MAGIC   equipment_type STRING,
# MAGIC   manufacturer STRING,
# MAGIC   model_number STRING,
# MAGIC   equipment_status STRING,
# MAGIC   criticality_level STRING,
# MAGIC   asset_value_usd DOUBLE,
# MAGIC   effective_start_date DATE,
# MAGIC   effective_end_date DATE,
# MAGIC   is_current BOOLEAN,
# MAGIC   version_number INT
# MAGIC ) USING DELTA;

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## DDL for Fact Tables

# COMMAND ----------

# DBTITLE 1,fact_sensor_readings
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS sparkwars.gold.fact_sensor_readings (
# MAGIC   sensor_reading_sk BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   equipment_sk      BIGINT NOT NULL,
# MAGIC   facility_sk       BIGINT NOT NULL,
# MAGIC   timestamp_sk      INT    NOT NULL,
# MAGIC   temperature        DOUBLE,
# MAGIC   vibration_x        DOUBLE,
# MAGIC   vibration_y        DOUBLE,
# MAGIC   device_id          String,
# MAGIC   equipment_type     String,
# MAGIC   pressure           DOUBLE,
# MAGIC   power_consumption  DOUBLE,
# MAGIC   rpm                INT,
# MAGIC   anomaly_score      DOUBLE,
# MAGIC   is_anomaly         BOOLEAN,
# MAGIC   _pipeline_run_id   STRING,
# MAGIC   created_at         TIMESTAMP
# MAGIC
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (facility_sk,timestamp_sk);

# COMMAND ----------

# DBTITLE 1,fact_production_output
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS sparkwars.gold.fact_production_output (
# MAGIC   production_sk BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   order_sk       BIGINT NOT NULL,
# MAGIC   equipment_sk   BIGINT NOT NULL,
# MAGIC   facility_sk    BIGINT NOT NULL,
# MAGIC   timestamp_sk   INT    NOT NULL,
# MAGIC   units_produced  INT,
# MAGIC   units_passed    INT,
# MAGIC   units_rejected  INT,
# MAGIC   defect_count    INT,
# MAGIC   defect_rate     DOUBLE,
# MAGIC   yield_rate      DOUBLE,
# MAGIC   _pipeline_run_id STRING,
# MAGIC   created_at       TIMESTAMP
# MAGIC
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (facility_sk,timestamp_sk);

# COMMAND ----------

# DBTITLE 1,fact_maintenance_events
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS sparkwars.gold.fact_maintenance_events (
# MAGIC   maintenance_sk BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   equipment_sk   BIGINT NOT NULL,
# MAGIC   technician_sk  BIGINT NOT NULL,
# MAGIC   facility_sk    BIGINT NOT NULL,
# MAGIC   timestamp_sk   INT    NOT NULL,
# MAGIC   downtime_minutes      INT,
# MAGIC   maintenance_cost_usd  DOUBLE,
# MAGIC   parts_cost_usd        DOUBLE,
# MAGIC   labor_cost_usd        DOUBLE,
# MAGIC   total_cost_usd        DOUBLE,
# MAGIC   maintenance_type  STRING,
# MAGIC   failure_code      STRING,
# MAGIC   _pipeline_run_id STRING,
# MAGIC   created_at       TIMESTAMP
# MAGIC
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (facility_sk, timestamp_sk);

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## ETL Code for populating Dimension Tables

# COMMAND ----------

# DBTITLE 1,dim_facility
# MAGIC %sql
# MAGIC INSERT INTO sparkwars.gold.dim_facility
# MAGIC (facility_id, facility_name, region, country, city, timezone, facility_status, is_active, created_at, updated_at)
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC     facility_id,
# MAGIC     CONCAT('Facility ', facility_id)      AS facility_name,
# MAGIC     'NA'                                   AS region,
# MAGIC     'USA'                                  AS country,
# MAGIC     'City_' || facility_id                 AS city,
# MAGIC     'UTC'                                  AS timezone,
# MAGIC     'ACTIVE'                               AS facility_status,
# MAGIC     true                                   AS is_active,
# MAGIC     current_timestamp()                    AS created_at,
# MAGIC     current_timestamp()                    AS updated_at
# MAGIC FROM sparkwars.silver.iot_telemetry;

# COMMAND ----------

# DBTITLE 1,dim_technician
# MAGIC %sql
# MAGIC INSERT INTO sparkwars.gold.dim_technician
# MAGIC (technician_id, technician_name, certification_level, region, is_active, created_at)
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC     maintenance_technician_id AS technician_id,
# MAGIC     CONCAT('Tech_', maintenance_technician_id) AS technician_name,
# MAGIC     'LEVEL_1' AS certification_level,
# MAGIC     'NA' AS region,
# MAGIC     true,
# MAGIC     current_timestamp()
# MAGIC FROM sparkwars.silver.maintenance_records
# MAGIC WHERE maintenance_technician_id IS NOT NULL;

# COMMAND ----------

# DBTITLE 1,dim_production_order
# MAGIC %sql
# MAGIC INSERT INTO sparkwars.gold.dim_production_order (
# MAGIC   order_id, 
# MAGIC   product_type, 
# MAGIC   priority, 
# MAGIC   planned_start_date, 
# MAGIC   planned_end_date, 
# MAGIC   facility_id, 
# MAGIC   created_at
# MAGIC )
# MAGIC SELECT DISTINCT
# MAGIC     order_id,
# MAGIC     product_sku AS product_type, -- Aliasing SKU to Type as we discussed
# MAGIC     priority,
# MAGIC     start_time AS planned_start_date, -- Mapping your timestamp to the start date
# MAGIC     NULL AS planned_end_date,         -- Setting this to NULL since data doesn't exist
# MAGIC     facility_id,
# MAGIC     current_timestamp()
# MAGIC FROM sparkwars.silver.production_orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO sparkwars.gold.dim_equipment
# MAGIC (equipment_id, facility_id, equipment_type, manufacturer, model_number,
# MAGIC  equipment_status, criticality_level, asset_value_usd,
# MAGIC  effective_start_date, effective_end_date, is_current, version_number)
# MAGIC
# MAGIC SELECT
# MAGIC     equipment_id,
# MAGIC     facility_id,
# MAGIC     equipment_type,
# MAGIC     manufacturer,
# MAGIC     model_number,
# MAGIC     equipment_status,
# MAGIC     criticality_level,
# MAGIC     asset_value_usd,
# MAGIC     effective_start_date,
# MAGIC     effective_end_date,
# MAGIC     is_current,
# MAGIC     version_number
# MAGIC FROM sparkwars.silver.equipment_master;

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## ETL Code for Fact Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO sparkwars.gold.fact_sensor_readings
# MAGIC (equipment_sk, facility_sk, timestamp_sk,
# MAGIC  temperature, vibration_x, vibration_y,device_id, 
# MAGIC  equipment_type, pressure, power_consumption, rpm,
# MAGIC  _pipeline_run_id, created_at)
# MAGIC
# MAGIC SELECT
# MAGIC     de.equipment_sk,
# MAGIC     df.facility_sk,
# MAGIC     dt.timestamp_sk,
# MAGIC
# MAGIC     s.temperature,
# MAGIC     s.vibration_x,
# MAGIC     s.vibration_y,
# MAGIC     s.device_id,
# MAGIC     s.equipment_type,
# MAGIC     s.pressure,
# MAGIC     s.power_consumption,
# MAGIC     s.rpm,
# MAGIC     
# MAGIC     'gold_load_001',
# MAGIC     current_timestamp()
# MAGIC
# MAGIC FROM sparkwars.silver.iot_telemetry s
# MAGIC
# MAGIC JOIN sparkwars.gold.dim_equipment de
# MAGIC   ON s.facility_id = de.facility_id
# MAGIC  AND s.event_date >= de.effective_start_date
# MAGIC  AND (s.event_date <= de.effective_end_date OR de.effective_end_date IS NULL)
# MAGIC
# MAGIC JOIN sparkwars.gold.dim_facility df
# MAGIC   ON s.facility_id = df.facility_id
# MAGIC
# MAGIC JOIN sparkwars.gold.dim_timestamp dt
# MAGIC   ON to_date(s.event_timestamp) = dt.date_day;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO sparkwars.gold.fact_production_output
# MAGIC (order_sk, equipment_sk, facility_sk, timestamp_sk,
# MAGIC  units_produced, units_passed, units_rejected,
# MAGIC  defect_count, defect_rate, yield_rate,
# MAGIC  _pipeline_run_id, created_at)
# MAGIC
# MAGIC SELECT
# MAGIC     dpo.order_sk,
# MAGIC     de.equipment_sk,
# MAGIC     df.facility_sk,
# MAGIC     dt.timestamp_sk,
# MAGIC
# MAGIC     p.quantity_ordered,
# MAGIC     (p.quantity_ordered - q.units_rejected) AS units_passed,
# MAGIC     q.units_rejected,
# MAGIC     q.units_rejected,
# MAGIC
# MAGIC     q.defect_rate,
# MAGIC     ((p.quantity_ordered - q.units_rejected) / p.quantity_ordered) AS yield_rate,
# MAGIC
# MAGIC     'gold_load_001',
# MAGIC     current_timestamp()
# MAGIC
# MAGIC FROM sparkwars.silver.production_orders p
# MAGIC JOIN sparkwars.silver.quality_inspection q
# MAGIC   ON p.order_id = q.order_id
# MAGIC
# MAGIC JOIN sparkwars.gold.dim_production_order dpo
# MAGIC   ON p.order_id = dpo.order_id
# MAGIC
# MAGIC JOIN sparkwars.gold.dim_equipment de
# MAGIC   ON p.equipment_id = de.equipment_id
# MAGIC
# MAGIC JOIN sparkwars.gold.dim_facility df
# MAGIC   ON p.facility_id = df.facility_id
# MAGIC
# MAGIC JOIN sparkwars.gold.dim_timestamp dt
# MAGIC   ON DATE(p.start_time) = dt.date_day

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO sparkwars.gold.fact_maintenance_events
# MAGIC (equipment_sk, technician_sk, facility_sk, timestamp_sk,
# MAGIC  downtime_minutes, maintenance_cost_usd,
# MAGIC  parts_cost_usd, labor_cost_usd, total_cost_usd,
# MAGIC  maintenance_type, failure_code,
# MAGIC  _pipeline_run_id, created_at)
# MAGIC
# MAGIC SELECT
# MAGIC     de.equipment_sk,
# MAGIC     dtc.technician_sk,
# MAGIC     df.facility_sk,
# MAGIC     dts.timestamp_sk,
# MAGIC
# MAGIC     m.downtime_minutes,
# MAGIC     m.maintenance_cost_usd,
# MAGIC     rand() * (m.maintenance_cost_usd / 10.0) AS parts_cost_usd,
# MAGIC     rand() * (m.maintenance_cost_usd / 10.0) AS labor_cost_usd,
# MAGIC     (parts_cost_usd + labor_cost_usd) AS total_cost_usd,
# MAGIC
# MAGIC     m.maintenance_type,
# MAGIC     m.failure_code,
# MAGIC
# MAGIC     'gold_load_001',
# MAGIC     current_timestamp()
# MAGIC
# MAGIC FROM sparkwars.silver.maintenance_records m
# MAGIC
# MAGIC JOIN sparkwars.gold.dim_equipment de
# MAGIC   ON m.equipment_id = de.equipment_id
# MAGIC
# MAGIC JOIN sparkwars.gold.dim_technician dtc
# MAGIC   ON m.maintenance_technician_id = dtc.technician_id
# MAGIC
# MAGIC JOIN sparkwars.gold.dim_facility df
# MAGIC   ON m.facility_id = df.facility_id
# MAGIC
# MAGIC JOIN sparkwars.gold.dim_timestamp dts
# MAGIC   ON DATE(m.start_datetime) = dts.date_day
# MAGIC
# MAGIC WHERE m.is_deleted = false;