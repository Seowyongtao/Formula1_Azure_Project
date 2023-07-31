# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest lap_times.csv file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "ergast")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

lap_times_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("lap", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

lap_times_df = spark.read\
                    .option("header", True)\
                    .schema(lap_times_schema)\
                    .csv(f"{raw_folder_path}/lap_times.csv")

# COMMAND ----------

lap_times_renamed_df = lap_times_df.withColumnRenamed("raceId", "race_id")\
                                   .withColumnRenamed("driverId", "driver_id")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

lap_times_final_df = add_ingestion_date(lap_times_renamed_df)\
                     .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.lap = src.lap"
merge_delta_data(lap_times_final_df, "f1_processed", "lap_times", processed_folder_path, merge_condition, "race_id")

# COMMAND ----------

dbutils.notebook.exit("Success")