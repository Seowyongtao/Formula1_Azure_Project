# Databricks notebook source
# MAGIC %md 
# MAGIC ### Ingest circuits.csv file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "ergast")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

circuits_schema = StructType(fields=[
    StructField("circuitId", IntegerType(), False),
    StructField("circuitRef", StringType(), True),
    StructField("name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("country", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lng", DoubleType(), True),
    StructField("alt", IntegerType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

circuits_df = spark.read\
                   .option("header", True)\
                   .schema(circuits_schema)\
                   .csv(f"{raw_folder_path}/circuits.csv")

# COMMAND ----------

from pyspark.sql.functions import col

circuits_selected_df = circuits_df.select(col("circuitID"),col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitID", "circuit_id")\
                                          .withColumnRenamed("circuitRef", "circuit_ref")\
                                          .withColumnRenamed("lat", "latitude")\
                                          .withColumnRenamed("lng", "longtitude")\
                                          .withColumnRenamed("alt", "altitude")

# COMMAND ----------

from pyspark.sql.functions import lit

circuits_column_added = circuits_renamed_df.withColumn("data_source", lit(v_data_source))

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_column_added)

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")

# COMMAND ----------

dbutils.notebook.exit("Success")