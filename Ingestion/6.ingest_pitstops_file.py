# Databricks notebook source
# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

dbutils.widgets.text("p-data-source", "")
v_data_source = dbutils.widgets.get("p-data-source")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, FloatType

pitstops_schema = StructType(fields = [
                                      StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("stop", IntegerType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("duration", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)]
                             )

# COMMAND ----------

pitstops_df = spark.read.option('multiline', True).json(f"{raw_folder_path}/pit_stops.json", schema=pitstops_schema)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, concat, lit, col

final_df = pitstops_df.withColumnRenamed("raceId", "race_id") \
                    .withColumnRenamed("driverId", "driver_id") \
                    .withColumn("ingestion_date", current_timestamp()) \
                    .withColumn("data_source", lit(v_data_source))


# COMMAND ----------

final_df.write.mode("overwrite").parquet(f'{processed_folder_path}/pit_stops')

# COMMAND ----------

