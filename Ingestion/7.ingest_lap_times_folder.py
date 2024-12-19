# Databricks notebook source
# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

dbutils.widgets.text("p-data-source", "")
v_data_source = dbutils.widgets.get("p-data-source")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, FloatType

lap_times_schema = StructType(fields = [
                                      StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)]
                             )

# COMMAND ----------

lap_times_df = spark.read.csv(f"{raw_folder_path}/lap_times", schema=lap_times_schema, header= True)

# or use lap_times_df = spark.read.csv("/mnt/formula1dlby63/raw/lap_times/lap_times_split_*.csv", schema=lap_times_schema, header= True)

# COMMAND ----------

display(lap_times_df.count()) #contains all the records across 5 split files

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, concat, lit, col

final_df = lap_times_df.withColumnRenamed("raceId", "race_id") \
                    .withColumnRenamed("driverId", "driver_id") \
                    .withColumn("ingestion_date", current_timestamp()) \
                    .withColumn("data_source", lit(v_data_source))


# COMMAND ----------

display(final_df)

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f'{processed_folder_path}/lap_times')

# COMMAND ----------

