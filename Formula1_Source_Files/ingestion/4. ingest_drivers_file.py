# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest drivers.csv file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "ergast")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

drivers_schema = StructType(fields=[
    StructField("driverId", IntegerType(), False),
    StructField("driverRef", StringType(), True), 
    StructField("number", IntegerType(), True),
    StructField("code", StringType(), True),
    StructField("forename", StringType(), True),
    StructField("surname", StringType(), True),
    StructField("dob", DateType(), True),
    StructField("nationality", StringType(), True),
    StructField("url", StringType(), True),
])

# COMMAND ----------

drivers_df = spark.read\
                  .option("header", True)\
                  .schema(drivers_schema)\
                  .csv(f"{raw_folder_path}/drivers.csv")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, concat, col, lit

drivers_columns_added_df = add_ingestion_date(drivers_df)\
                           .withColumn("name", concat(col("forename"), lit(" "), col("surname")))\
                           .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

drivers_renamed_df = drivers_columns_added_df.withColumnRenamed("driverId", "driver_id")\
                                             .withColumnRenamed("driverRef", "driver_ref")

# COMMAND ----------

drivers_final_df = drivers_renamed_df.drop(col("url"))

# COMMAND ----------

drivers_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

dbutils.notebook.exit("Success")