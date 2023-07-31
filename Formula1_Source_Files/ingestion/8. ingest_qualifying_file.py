# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest qualifying.csv file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "ergast")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

qualifying_schema = StructType(fields=[
    StructField("qualifyId", IntegerType(), False),
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("constructorId", IntegerType(), True),
    StructField("number", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("q1", StringType(), True),
    StructField("q2", StringType(), True),
    StructField("q3", StringType(), True)
])

# COMMAND ----------

qualifying_df = spark.read\
                     .option("header", True)\
                     .schema(qualifying_schema)\
                     .csv(f"{raw_folder_path}/qualifying.csv")

# COMMAND ----------

qualifying_renamed_df = qualifying_df.withColumnRenamed("qualifyId", "qualify_id")\
                                     .withColumnRenamed("raceId", "race_id")\
                                     .withColumnRenamed("driverId", "driver_id")\
                                     .withColumnRenamed("constructorId", "constructor_id")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

qualifying_final_df = add_ingestion_date(qualifying_renamed_df)\
                      .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.qualify_id = src.qualify_id"
merge_delta_data(qualifying_final_df, "f1_processed", "qualifying", processed_folder_path, merge_condition, "race_id")

# COMMAND ----------

dbutils.notebook.exit("Success")