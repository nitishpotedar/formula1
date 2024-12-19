# Databricks notebook source
# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

dbutils.widgets.text("p-data-source", "")
v_data_source = dbutils.widgets.get("p-data-source")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

name_schema = StructType(fields = [StructField("forename", StringType(), True),
                                   StructField("surname", StringType(), True)])

drivers_schema = StructType(fields = [StructField("driverId", IntegerType(), False),
                                      StructField("driverRef", StringType(), True),
                                      StructField("number", IntegerType(), False),
                                      StructField("code", StringType(), True),
                                      StructField("name", name_schema),
                                      StructField("dob", DateType(), True),
                                      StructField("nationality", StringType(), True),
                                      StructField("url", StringType(), True)]
                             )

# COMMAND ----------

drivers_df = spark.read.json(f"{raw_folder_path}/drivers.json", schema=drivers_schema)

# COMMAND ----------

dropped_df = drivers_df.drop("url")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, concat, lit, col

final_df = dropped_df.withColumnRenamed("driverId", "driver_id") \
                                .withColumnRenamed("driverRef", "driver_ref") \
                                .withColumnRenamed("constructorRef", "constructor_ref") \
                                .withColumn("name", concat(col('name.forename'), lit(' '), col('name.surname'))) \
                                .withColumn("ingestion_date", current_timestamp()) \
                                .withColumn("data_source", lit(v_data_source))


# COMMAND ----------

final_df.write.mode("overwrite").parquet(f'{processed_folder_path}/drivers')

# COMMAND ----------

drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers")