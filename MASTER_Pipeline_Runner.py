# Databricks notebook source
import uuid
import time
from datetime import datetime

pipeline_run_id = str(uuid.uuid4())
start_time = time.time()

print("======================================")
print("🚀 Smart Manufacturing Lakehouse")
print(f"Run ID : {pipeline_run_id}")
print(f"Start  : {datetime.now()}")
print("======================================")


# COMMAND ----------

def run_notebook(step_name, path):
    print(f"\n🔹 Running: {step_name}")
    step_start = time.time()
    
    try:
        dbutils.notebook.run(path, timeout_seconds=0)
        print(f"✅ {step_name} completed in {round(time.time() - step_start, 2)} sec")
    except Exception as e:
        print(f"❌ {step_name} FAILED")
        print(str(e))
        raise

# COMMAND ----------

run_notebook("Create Databases", "./Create_Databases")
run_notebook("Data Generator", "./Data_Generator")

run_notebook("Bronze Layer", "./Bronze_Layer_Data_Ingestion")

run_notebook("Silver Layer", "./Silver_Layer_Cleansing_and_Validation")
run_notebook("DQ Reporting", "./DQ_Reporting")

run_notebook("Gold Facts & Dimensions", "./Gold_Facts_and_Dim")
run_notebook("Security Simulation", "./Security_Simulation")

run_notebook("Optimization Script", "./Optimization_Script")

run_notebook("Testing & Validation", "./Testing_and_Validation")

# COMMAND ----------

total_time = round(time.time() - start_time, 2)

print("\n======================================")
print("🎉 PIPELINE COMPLETED SUCCESSFULLY")
print(f"Run ID : {pipeline_run_id}")
print(f"Total Execution Time : {total_time} seconds")
print("======================================")