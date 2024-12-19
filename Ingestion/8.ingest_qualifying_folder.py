# Databricks notebook source
# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

dbutils.widgets.text("p-data-source", "")
v_data_source = dbutils.widgets.get("p-data-source")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, FloatType

qualifying_schema = StructType(fields = [
                                      StructField("qualifyId", IntegerType(), False),
                                      StructField("raceId", IntegerType(), True),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("constructorId", IntegerType(), True),
                                      StructField("number", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("q1", StringType(), True),
                                      StructField("q2", StringType(), True),
                                      StructField("q3", StringType(), True)]
                             )

# COMMAND ----------

qualifying_df = spark.read.option('multiline', True).json(f"{raw_folder_path}/qualifying", schema=qualifying_schema)

# COMMAND ----------

display(qualifying_df.count()) #contains all the records across 2 split files

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

final_df = qualifying_df.withColumnRenamed("qualifyId", "qualify_id") \
                    .withColumnRenamed("raceId", "race_id") \
                    .withColumnRenamed("driverId", "driver_id") \
                    .withColumnRenamed("constructorId", "constructor_id") \
                    .withColumn("ingestion_date", current_timestamp()) \
                    .withColumn("data_source", lit(v_data_source))


# COMMAND ----------

final_df.write.mode("overwrite").parquet(f'{processed_folder_path}/qualifying')

# COMMAND ----------

