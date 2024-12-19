# Databricks notebook source
# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

dbutils.widgets.text("p-data-source", "")
v_data_source = dbutils.widgets.get("p-data-source")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, FloatType

results_schema = StructType(fields = [StructField("resultId", IntegerType(), False),
                                      StructField("raceId", IntegerType(), True),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("constructorId", IntegerType(), True),
                                      StructField("number", IntegerType(), True),
                                      StructField("grid", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("positionText", StringType(), True),
                                      StructField("positionOrder", IntegerType(), True),
                                      StructField("points", FloatType(), True),
                                      StructField("laps", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True),
                                      StructField("fastestLap", IntegerType(), True),
                                      StructField("rank", IntegerType(), True),
                                      StructField("fastestLapTime", StringType(), True),
                                      StructField("fastestLapSpeed", StringType(), True),
                                      StructField("statusId", IntegerType(), True),]
                             )

# COMMAND ----------

results_df = spark.read.json(f"{raw_folder_path}/results.json", schema=results_schema)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, concat, lit, col

final_df = results_df.withColumnRenamed("resultId", "result_id") \
                    .withColumnRenamed("raceId", "race_id") \
                    .withColumnRenamed("driverId", "driver_id") \
                    .withColumnRenamed("constructorId", "constructor_id") \
                    .withColumnRenamed("positionText", "position_text") \
                    .withColumnRenamed("positionOrder", "position_order") \
                    .withColumnRenamed("fastestLap", "fastest_lap") \
                    .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
                    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
                    .withColumn("ingestion_date", current_timestamp()) \
                    .drop("statusId") \
                    .withColumn("data_source", lit(v_data_source))


# COMMAND ----------

final_df.write.mode("overwrite").partitionBy('race_id').parquet(f'{processed_folder_path}/results')

# COMMAND ----------

results_df = spark.read.parquet(f"{processed_folder_path}/results")