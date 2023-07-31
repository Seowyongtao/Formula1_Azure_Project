# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest races.csv file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "ergast")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, TimestampType

races_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("year", IntegerType(), True),
    StructField("round", IntegerType(), True),
    StructField("circuitId", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("date", StringType(), True),
    StructField("time", StringType(), True),
    StructField("url", StringType(), True),
    StructField("fp1_date", StringType(), True),
    StructField("fp1_time", StringType(), True),
    StructField("fp2_date", StringType(), True),
    StructField("fp2_time", StringType(), True),
    StructField("fp3_date", StringType(), True),
    StructField("fp3_time", StringType(), True),
    StructField("quali_date", StringType(), True),
    StructField("quali_time", StringType(), True),
    StructField("sprint_date", StringType(), True),
    StructField("sprint_time", StringType(), True),
])

# COMMAND ----------

races_df = spark.read\
                .option("header", True)\
                .schema(races_schema)\
                .csv(f"{raw_folder_path}/races.csv")

# COMMAND ----------

from pyspark.sql.functions import col, concat, lit, to_timestamp, current_timestamp

races_add_race_timestamp_df = races_df.withColumn("race_timestamp", to_timestamp(concat(col("date"), lit(" "), col("time")), 'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

races_add_ingestion_date_df = add_ingestion_date(races_add_race_timestamp_df)

# COMMAND ----------

from pyspark.sql.functions import col

races_selected_df = races_add_ingestion_date_df.select(col("raceId"), col("year"), col("round"), col("circuitId"), col("name"),col("race_timestamp"), col("ingestion_date") )


# COMMAND ----------

races_column_added = races_selected_df.withColumn("data_source", lit(v_data_source))

# COMMAND ----------

races_final_df = races_column_added.withColumnRenamed("raceId", "race_id")\
                                   .withColumnRenamed("year", "race_year")\
                                   .withColumnRenamed("circuitId", "circuit_id")

# COMMAND ----------

races_final_df.write.mode("overwrite").partitionBy('race_year').format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

dbutils.notebook.exit("Success")