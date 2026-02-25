# Databricks notebook source
# MAGIC %md
# MAGIC ## IoT Sensor Telemetry Synthetic Generator (Streaming JSON)

# COMMAND ----------

# DBTITLE 1,delete
# # Replace 'catalog', 'schema', and 'volume_name' with your actual names
# dbutils.fs.mkdirs("/Volumes/sparkwars/bronze/bronze_volume/iot_telemetry/")

# IOT_LANDING_PATH = "/Volumes/sparkwars/bronze/bronze_volume/iot_telemetry/"

# from pyspark.sql.functions import (
#     rand,
#     expr,
#     current_timestamp,
#     col,
#     when,
#     lit,
#     concat,
#     floor
# )

# def generate_iot_batch(num_records=10000):

#     df = spark.range(0, num_records)

#     df = df.select(
#         # Random facility
#         expr("""
#             CASE floor(rand()*4)
#                 WHEN 0 THEN 'FAC-NA-001'
#                 WHEN 1 THEN 'FAC-NA-002'
#                 WHEN 2 THEN 'FAC-EU-003'
#                 ELSE 'FAC-APAC-002'
#             END
#         """).alias("facility_id"),

#         # Random equipment type
#         expr("""
#             CASE floor(rand()*4)
#                 WHEN 0 THEN 'CNC_MACHINE'
#                 WHEN 1 THEN 'HYDRAULIC_PRESS'
#                 WHEN 2 THEN 'CENTRIFUGAL_PUMP'
#                 ELSE 'WELDING_ROBOT'
#             END
#         """).alias("equipment_type"),

#         # Timestamp within last hour
#         #(current_timestamp() - expr("INTERVAL floor(rand()*3600) SECOND")).alias("event_timestamp"),
#         expr("timestampadd(SECOND, -cast(rand()*3600 as int), current_timestamp())").alias("event_timestamp"),

#         # Metrics
#         (rand()*40 + 50).alias("temperature"),
#         (rand()*4 + 0.5).alias("vibration_x"),
#         (rand()*4 + 0.5).alias("vibration_y"),
#         (rand()*150 + 100).alias("pressure"),
#         (rand()*17 + 8).alias("power_consumption"),
#         floor(rand()*3200 + 800).cast("int").alias("rpm")
#     )

#     # Create simple device_id
#     df = df.withColumn(
#         "device_id",
#         concat(
#             lit("DEV-"),
#             floor(rand()*10000).cast("int")
#         )
#     )

#     return df


# def write_iot_data(total_records=50000, batch_size=10000):

#     batches = total_records // batch_size

#     for i in range(batches):

#         df = generate_iot_batch(batch_size)

#         df.coalesce(1).write.mode("append").json(IOT_LANDING_PATH)

#         print(f"Written batch {i+1}")


# write_iot_data(50000, 10000)

# COMMAND ----------

dbutils.fs.mkdirs("/Volumes/sparkwars/bronze/bronze_volume/iot_telemetry/")

# COMMAND ----------

IOT_LANDING_PATH = "/Volumes/sparkwars/bronze/bronze_volume/iot_telemetry/"

# COMMAND ----------

from pyspark.sql.functions import (
    rand,
    expr,
    current_timestamp,
    col,
    when,
    lit,
    concat,
    floor
)

def generate_iot_batch(num_records=10000):

    df = spark.range(0, num_records)

    df = df.select(
        # Random facility
        expr("""
            CASE floor(rand()*4)
                WHEN 0 THEN 'FAC-NA-001'
                WHEN 1 THEN 'FAC-NA-002'
                WHEN 2 THEN 'FAC-EU-003'
                ELSE 'FAC-APAC-002'
            END
        """).alias("facility_id"),

        # Random equipment type
        expr("""
            CASE floor(rand()*4)
                WHEN 0 THEN 'CNC_MACHINE'
                WHEN 1 THEN 'HYDRAULIC_PRESS'
                WHEN 2 THEN 'CENTRIFUGAL_PUMP'
                ELSE 'WELDING_ROBOT'
            END
        """).alias("equipment_type"),

        # Timestamp within last hour
        #(current_timestamp() - expr("INTERVAL floor(rand()*3600) SECOND")).alias("event_timestamp"),
        expr("timestampadd(SECOND, -cast(rand()*3600 as int), current_timestamp())").alias("event_timestamp"),
        # Metrics
        (rand()*40 + 50).alias("temperature"),
        (rand()*4 + 0.5).alias("vibration_x"),
        (rand()*4 + 0.5).alias("vibration_y"),
        (rand()*150 + 100).alias("pressure"),
        (rand()*17 + 8).alias("power_consumption"),
        floor(rand()*3200 + 800).cast("int").alias("rpm")
    )

    # Create simple device_id
    df = df.withColumn(
        "device_id",
        concat(
            lit("DEV-"),
            floor(rand()*10000).cast("int")
        )
    )

    return df

# COMMAND ----------

def write_iot_data(total_records=50000, batch_size=10000):

    batches = total_records // batch_size

    for i in range(batches):

        df = generate_iot_batch(batch_size)

        df.coalesce(1).write.mode("append").json(IOT_LANDING_PATH)

        print(f"Written batch {i+1}")

# COMMAND ----------

# DBTITLE 1,Run write_iot_data in Python
write_iot_data(50000, 10000)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Production Orders (Parquet)

# COMMAND ----------

PROD_LANDING_PATH = "/Volumes/sparkwars/bronze/bronze_volume/production_orders/"

# COMMAND ----------

dbutils.fs.mkdirs(PROD_LANDING_PATH)

# COMMAND ----------

from pyspark.sql.functions import (
    rand,
    expr,
    current_timestamp,
    col,
    when,
    lit,
    concat,
    floor
)

def generate_production_orders(num_records=1000):

    df = spark.range(1, num_records + 1)

    # Facility selection
    df = df.withColumn(
        "facility_id",
        expr("""
            CASE floor(rand()*4)
                WHEN 0 THEN 'FAC-NA-001'
                WHEN 1 THEN 'FAC-NA-002'
                WHEN 2 THEN 'FAC-EU-003'
                ELSE 'FAC-APAC-002'
            END
        """)
    )

    # Equipment mapped loosely to facility
    df = df.withColumn(
        "equipment_id",
        concat(lit("EQ-"), floor(rand()*9999).cast("int"))
    )

    # SKU selection
    df = df.withColumn(
        "product_sku",
        expr("""
            CASE floor(rand()*3)
                WHEN 0 THEN 'SKU-XYZ-500'
                WHEN 1 THEN 'SKU-ABC-250'
                ELSE 'SKU-DEF-100'
            END
        """)
    )

    # Quantity distribution
    df = df.withColumn(
        "quantity_ordered",
        floor(rand()*2500 + 100).cast("int")
    )

    # Start time within last 7 days
    df = df.withColumn(
        "start_time",
        expr("timestampadd(SECOND, -cast(rand()*604800 as int), current_timestamp())")
    )

    # Priority distribution (20% HIGH, 50% MEDIUM, 30% LOW)
    df = df.withColumn(
        "priority",
        when(rand() < 0.2, "HIGH")
        .when(rand() < 0.7, "MEDIUM")
        .otherwise("LOW")
    )

    # Order ID
    df = df.withColumn(
        "order_id",
        concat(lit("ORD-2026-"), col("id"))
    ).drop("id")

    return df.select(
        "order_id",
        "facility_id",
        "product_sku",
        "quantity_ordered",
        "start_time",
        "equipment_id",
        "priority"
    )

# COMMAND ----------

def write_production_orders(total_records=1000):

    df = generate_production_orders(total_records)

    df.coalesce(1).write.mode("append").parquet(PROD_LANDING_PATH)

    print(f"{total_records} Production Orders written.")

# COMMAND ----------

write_production_orders(1000)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Maintenance Records

# COMMAND ----------

MAINT_LANDING_PATH = "/Volumes/sparkwars/bronze/bronze_volume/maintenance_records/"
dbutils.fs.mkdirs(MAINT_LANDING_PATH)

# COMMAND ----------

from pyspark.sql.functions import (
    rand,
    expr,
    current_timestamp,
    col,
    when,
    lit,
    concat,
    floor
)

def generate_maintenance_records(num_base_records=500):

    base = spark.range(1, num_base_records + 1)

    # Generate INSERT events
    df_insert = base.select(
        concat(lit("MNT-2026-"), col("id")).alias("maintenance_id"),

        concat(lit("EQ-"), floor(rand()*9999).cast("int")).alias("equipment_id"),

        expr("""
            CASE floor(rand()*4)
                WHEN 0 THEN 'FAC-NA-001'
                WHEN 1 THEN 'FAC-NA-002'
                WHEN 2 THEN 'FAC-EU-003'
                ELSE 'FAC-APAC-002'
            END
        """).alias("facility_id"),

        expr("""
            CASE floor(rand()*3)
                WHEN 0 THEN 'CORRECTIVE'
                WHEN 1 THEN 'PREVENTIVE'
                ELSE 'PREDICTIVE'
            END
        """).alias("maintenance_type"),

        concat(lit("TECH-"), floor(rand()*1000).cast("int")).alias("maintenance_technician_id"),

        # Start time within last 7 days
        expr(
            "timestampadd(SECOND, -cast(rand()*604800 as int), current_timestamp())"
        ).alias("start_datetime"),

        floor(rand()*300 + 30).cast("int").alias("downtime_minutes"),

        concat(lit("FC-"), floor(rand()*100).cast("int")).alias("failure_code"),

        concat(lit("PART-"), floor(rand()*50).cast("int")).alias("parts_replaced"),

        (rand()*5000 + 100).alias("maintenance_cost_usd"),

        concat(lit("WO-2026-"), col("id")).alias("work_order_id"),

        lit("INSERT").alias("_cdc_operation"),

        current_timestamp().alias("_cdc_timestamp"),

        col("id").alias("_lsn")
    )

    # End time = 0–5 hours after start time
    df_insert = df_insert.withColumn(
        "end_datetime",
        expr("timestampadd(SECOND, cast(rand()*18000 as int), start_datetime)")
    )

    # Add random column for deterministic 20% updates
    df_insert = df_insert.withColumn("rand_val", rand())

    # Generate UPDATE events (20%)
    df_update = (
        df_insert
        .filter(col("rand_val") < 0.2)
        .drop("rand_val")
        .withColumn("_cdc_operation", lit("UPDATE"))
        .withColumn("_cdc_timestamp", current_timestamp())
        .withColumn("maintenance_cost_usd", col("maintenance_cost_usd") + 500)
        .withColumn("_lsn", col("_lsn") + num_base_records)
    )

    # Drop helper column from inserts
    df_insert = df_insert.drop("rand_val")

    # Combine INSERT + UPDATE events
    final_df = df_insert.unionByName(df_update)

    return final_df

# COMMAND ----------

def write_maintenance_records(num_base_records=500):

    df = generate_maintenance_records(num_base_records)

    df.coalesce(1).write.mode("append").parquet(MAINT_LANDING_PATH)

    print(f"Generated {df.count()} CDC maintenance records.")

# COMMAND ----------

# DBTITLE 1,why is it generating 599 records
write_maintenance_records(500)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality Inspection

# COMMAND ----------

QUALITY_LANDING_PATH = "/Volumes/sparkwars/bronze/bronze_volume/quality_inspection/"
dbutils.fs.mkdirs(QUALITY_LANDING_PATH)

# COMMAND ----------

from pyspark.sql.functions import (
    rand,
    expr,
    current_timestamp,
    col,
    when,
    lit,
    concat,
    floor
)

def generate_quality_inspection(num_records=2000):

    df = spark.range(1, num_records + 1)

    # Generate base dataset
    df = df.select(
        concat(lit("INSP-2026-"), col("id")).alias("inspection_id"),

        concat(lit("ORD-2026-"), floor(rand()*2000 + 1).cast("int")).alias("order_id"),

        concat(lit("EQ-"), floor(rand()*9999).cast("int")).alias("equipment_id"),

        expr("""
            CASE floor(rand()*4)
                WHEN 0 THEN 'FAC-NA-001'
                WHEN 1 THEN 'FAC-NA-002'
                WHEN 2 THEN 'FAC-EU-003'
                ELSE 'FAC-APAC-002'
            END
        """).alias("facility_id"),

        concat(lit("EMP-"), floor(rand()*1000).cast("int")).alias("inspector_employee_id"),

        # FIXED: timestamp within last 7 days
        expr(
            "timestampadd(SECOND, -cast(rand()*604800 as int), current_timestamp())"
        ).alias("inspection_timestamp"),

        expr("""
            CASE floor(rand()*3)
                WHEN 0 THEN 'SKU-XYZ-500'
                WHEN 1 THEN 'SKU-ABC-250'
                ELSE 'SKU-DEF-100'
            END
        """).alias("product_sku"),

        concat(lit("BAT-2026-"), col("id")).alias("batch_id"),

        floor(rand()*200 + 50).cast("int").alias("units_inspected")
    )

    # Deterministic random factor for rejection %
    df = df.withColumn("rand_factor", rand())

    # Rejected units (0–20%)
    df = df.withColumn(
        "units_rejected",
        floor(col("units_inspected") * col("rand_factor") * 0.2).cast("int")
    )

    # Passed units
    df = df.withColumn(
        "units_passed",
        col("units_inspected") - col("units_rejected")
    )

    # Defect logic
    df = df.withColumn(
        "defect_type",
        when(col("units_rejected") > 0,
             expr("""
                CASE floor(rand()*3)
                    WHEN 0 THEN 'DIMENSIONAL_ERROR'
                    WHEN 1 THEN 'SURFACE_DEFECT'
                    ELSE 'POROSITY'
                END
             """)
        ).otherwise(lit(None))
    )

    df = df.withColumn(
        "defect_severity",
        when(col("units_rejected") > 0,
             expr("""
                CASE floor(rand()*3)
                    WHEN 0 THEN 'MINOR'
                    WHEN 1 THEN 'MAJOR'
                    ELSE 'CRITICAL'
                END
             """)
        ).otherwise(lit(None))
    )

    df = df.withColumn(
        "inspection_method",
        expr("""
            CASE floor(rand()*3)
                WHEN 0 THEN 'VISUAL'
                WHEN 1 THEN 'CMM'
                ELSE 'X-RAY'
            END
        """)
    )

    # Result logic
    df = df.withColumn(
        "result",
        when(col("units_rejected") == 0, "PASS")
        .when(col("defect_severity") == "CRITICAL", "FAIL")
        .otherwise("PASS")
    )

    # Cleanup helper column
    df = df.drop("rand_factor")

    return df.select(
        "inspection_id",
        "order_id",
        "equipment_id",
        "facility_id",
        "inspector_employee_id",
        "inspection_timestamp",
        "product_sku",
        "batch_id",
        "units_inspected",
        "units_passed",
        "units_rejected",
        "defect_type",
        "defect_severity",
        "inspection_method",
        "result"
    )

# COMMAND ----------

def write_quality_inspection(num_records=2000):

    df = generate_quality_inspection(num_records)

    df.coalesce(1).write.mode("append") \
        .option("header", True) \
        .csv(QUALITY_LANDING_PATH)

    print(f"{num_records} Quality Inspection records written.")

# COMMAND ----------

write_quality_inspection(2000)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Equipment Master

# COMMAND ----------

EQUIPMENT_DELTA_PATH = "/Volumes/sparkwars/bronze/bronze_volume/equipment_master/"
dbutils.fs.mkdirs(EQUIPMENT_DELTA_PATH)

# COMMAND ----------

from pyspark.sql.functions import (
    rand,
    expr,
    current_date,
    col,
    when,
    lit,
    concat,
    floor
)

def generate_equipment_master(num_records=50):

    df = spark.range(1, num_records + 1)

    # Facility assignment
    df = df.withColumn(
        "facility_id",
        expr("""
            CASE floor(rand()*4)
                WHEN 0 THEN 'FAC-NA-001'
                WHEN 1 THEN 'FAC-NA-002'
                WHEN 2 THEN 'FAC-EU-003'
                ELSE 'FAC-APAC-002'
            END
        """)
    )

    # Equipment type
    df = df.withColumn(
        "equipment_type",
        expr("""
            CASE floor(rand()*4)
                WHEN 0 THEN 'CNC_MACHINE'
                WHEN 1 THEN 'HYDRAULIC_PRESS'
                WHEN 2 THEN 'CENTRIFUGAL_PUMP'
                ELSE 'WELDING_ROBOT'
            END
        """)
    )

    # Equipment ID
    df = df.withColumn(
        "equipment_id",
        concat(lit("EQ-"), col("id"))
    )

    # Serial number
    df = df.withColumn(
        "equipment_serial_number",
        concat(lit("SN-"), col("equipment_type"), lit("-"), col("id"))
    )

    # Manufacturer mapping
    df = df.withColumn(
        "manufacturer",
        expr("""
            CASE equipment_type
                WHEN 'CNC_MACHINE' THEN 'Haas Automation'
                WHEN 'HYDRAULIC_PRESS' THEN 'Schuler Group'
                WHEN 'CENTRIFUGAL_PUMP' THEN 'Grundfos'
                ELSE 'KUKA'
            END
        """)
    )

    # Model number
    df = df.withColumn(
        "model_number",
        concat(lit("MODEL-"), floor(rand()*1000))
    )

    # FIXED: Manufacture date (up to ~5.5 years ago)
    df = df.withColumn(
        "manufacture_date",
        expr("date_sub(current_date(), cast(rand()*2000 as int))")
    )

    # FIXED: Installation date (0–90 days after manufacture)
    df = df.withColumn(
        "installation_date",
        expr("date_add(manufacture_date, cast(rand()*90 as int))")
    )

    # Technical specs
    df = df.withColumn("rated_capacity", floor(rand()*1000 + 200))
    df = df.withColumn("operating_voltage", floor(rand()*200 + 220))
    df = df.withColumn("max_temperature_c", rand()*50 + 50)
    df = df.withColumn("max_vibration_hz", rand()*40 + 20)
    df = df.withColumn("maintenance_interval_days", floor(rand()*180 + 30))

    # Deterministic random columns for distributions
    df = df.withColumn("status_rand", rand())
    df = df.withColumn("critical_rand", rand())

    # Status distribution (85/10/5 realistic split)
    df = df.withColumn(
        "equipment_status",
        when(col("status_rand") < 0.85, "ACTIVE")
        .when(col("status_rand") < 0.95, "UNDER_MAINTENANCE")
        .otherwise("INACTIVE")
    )

    # Asset value
    df = df.withColumn("asset_value_usd", rand()*500000 + 20000)

    # Criticality level (30/40/30 split)
    df = df.withColumn(
        "criticality_level",
        when(col("critical_rand") < 0.3, "CRITICAL")
        .when(col("critical_rand") < 0.7, "HIGH")
        .otherwise("MEDIUM")
    )

    # Drop helper columns
    df = df.drop("id", "status_rand", "critical_rand")

    return df.select(
        "equipment_id",
        "equipment_serial_number",
        "facility_id",
        "equipment_type",
        "manufacturer",
        "model_number",
        "manufacture_date",
        "installation_date",
        "rated_capacity",
        "operating_voltage",
        "max_temperature_c",
        "max_vibration_hz",
        "maintenance_interval_days",
        "equipment_status",
        "asset_value_usd",
        "criticality_level"
    )

# COMMAND ----------

def write_equipment_master(num_records=50):

    df = generate_equipment_master(num_records)

    df.write.mode("overwrite").format("delta").save(EQUIPMENT_DELTA_PATH)

    print(f"{num_records} Equipment Master records written to Delta.")

# COMMAND ----------

write_equipment_master(50)