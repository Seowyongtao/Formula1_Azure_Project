# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest results.csv file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "ergast")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

results_schema = StructType(fields=[
    StructField("resultId", IntegerType(), False),
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("constructorId", IntegerType(), True),
    StructField("number", IntegerType(), True),
    StructField("grid", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("positionText", StringType(), True),
    StructField("positionOrder", IntegerType(), True),
    StructField("points", DoubleType(), True),
    StructField("laps", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True),
    StructField("fastestLap", IntegerType(), True),
    StructField("rank", IntegerType(), True),
    StructField("fastestLaptime", StringType(), True),
    StructField("fastestLapSpeed", StringType(), True),
    StructField("statusId", IntegerType(), True)
])

# COMMAND ----------

results_df = spark.read\
                  .option("header", True)\
                  .schema(results_schema)\
                  .csv(f"{raw_folder_path}/results.csv")

# COMMAND ----------

from pyspark.sql.functions import col

results_dropped_df = results_df.drop(col("statusId"))

# COMMAND ----------

results_renamed_df = results_dropped_df.withColumnRenamed("resultId", "result_id")\
                                       .withColumnRenamed("raceId", "race_id")\
                                       .withColumnRenamed("driverId", "driver_id")\
                                       .withColumnRenamed("constructorId", "constructor_id")\
                                       .withColumnRenamed("positionText", "position_text")\
                                       .withColumnRenamed("positionOrder", "position_order")\
                                       .withColumnRenamed("fastestLap", "fastest_lap")\
                                       .withColumnRenamed("fastestLapTime", "fastest_lap_time")\
                                       .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

results_final_df = add_ingestion_date(results_renamed_df)\
                   .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

results_deduped_df = results_final_df.dropDuplicates(["race_id", "driver_id"])

# COMMAND ----------

merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
merge_delta_data(results_deduped_df, "f1_processed", "results", processed_folder_path, merge_condition, "race_id")

# COMMAND ----------

dbutils.notebook.exit("Success")