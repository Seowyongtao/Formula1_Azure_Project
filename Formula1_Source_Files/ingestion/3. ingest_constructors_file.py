# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest constructors.csv file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "ergast")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructors_df = spark.read \
                       .option("header", True)\
                       .schema(constructors_schema) \
                       .csv(f"{raw_folder_path}/constructors.csv")

# COMMAND ----------

from pyspark.sql.functions import col

constructors_dropped_df = constructors_df.drop(col("url"))

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

constructors_renamed_df = constructors_dropped_df\
                          .withColumnRenamed("constructorId", "constructor_id")\
                          .withColumnRenamed("constructorRef", "constructor_ref")
                        

# COMMAND ----------

constructors_final_df = add_ingestion_date(constructors_renamed_df)\
                        .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

constructors_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructors")

# COMMAND ----------

dbutils.notebook.exit("Success")