# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest pitstops.csv file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "ergast")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

pit_stops_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("stop", StringType(), True),
    StructField("lap", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("duration", StringType(), True),
    StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

pit_stops_df = spark.read\
                    .option("header", True)\
                    .schema(pit_stops_schema)\
                    .csv(f"{raw_folder_path}/pit_stops.csv")

# COMMAND ----------

pit_stops_renamed_df = pit_stops_df.withColumnRenamed("raceId", "race_id")\
                                   .withColumnRenamed("driverId", "driver_id")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

pit_stops_final_df = add_ingestion_date(pit_stops_renamed_df)\
                  .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.stop = src.stop"
merge_delta_data(pit_stops_final_df, "f1_processed", "pit_stops", processed_folder_path, merge_condition, "race_id")

# COMMAND ----------

dbutils.notebook.exit("Success")