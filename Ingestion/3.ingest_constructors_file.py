# Databricks notebook source
# MAGIC %run "../includes/configuration"
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p-data-source", "")
v_data_source = dbutils.widgets.get("p-data-source")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

constructors_schema = StructType(fields = [StructField("constructorId", IntegerType(), False),
                                      StructField("constructorRef", StringType(), True),
                                      StructField("name", StringType(), True),
                                      StructField("nationality", StringType(), True),
                                      StructField("url", StringType(), True)]
                             )

# COMMAND ----------

constructors_df = spark.read.json(f"{raw_folder_path}/constructors.json", schema=constructors_schema)

# COMMAND ----------

dropped_df = constructors_df.drop("url")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

final_df = dropped_df.withColumnRenamed("constructorId", "constructor_id") \
                                .withColumnRenamed("constructorRef", "constructor_ref") \
                                .withColumnRenamed("name", "constructor_name") \
                                .withColumn("ingestion_date", current_timestamp()) \
                                .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f'{processed_folder_path}/constructors')

# COMMAND ----------

constructors_df = spark.read.parquet(f"{processed_folder_path}/constructors")