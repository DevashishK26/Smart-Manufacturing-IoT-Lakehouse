# Databricks notebook source
# MAGIC %sql
# MAGIC SHOW CATALOGS;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP CATALOG IF EXISTS sparkwars CASCADE;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG if not EXISTS sparkwars;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS sparkwars.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS sparkwars.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS sparkwars.gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS sparkwars.bronze.bronze_volume;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SCHEMAS IN sparkwars;