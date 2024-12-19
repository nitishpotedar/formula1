# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p-data-source", "")
v_data_source = dbutils.widgets.get("p-data-source")

# COMMAND ----------

from pyspark.sql.types import StructType, StringType, IntegerType, DateType, TimestampType, StructField

races_schema = StructType(fields = [    StructField("raceId", IntegerType(), False),
                                        StructField("year", IntegerType(), True),
                                        StructField("round", IntegerType(), True),                                 
                                        StructField("circuitId", IntegerType(), True),
                                        StructField("name", StringType(), True), 
                                        StructField("date", DateType(), True), 
                                        StructField("time", StringType(), True), 
                                        StructField("url", StringType(), True) 
                                    ]
                             )

# COMMAND ----------

df = spark.read.csv(f"{raw_folder_path}/races.csv", schema = races_schema, header = True)

# COMMAND ----------

# from pyspark.sql.functions import current_timestamp, to_timestamp, concat, lit, col

# selected_df = races_df.withColumnRenamed("raceId", "race_id") \
#                     .withColumnRenamed("circuitId", "circuit_id") \
#                     .withColumn("ingestion_date", current_timestamp()) \
#                     .withColumn("race_timestamp", to_timestamp(concat(col("date"), lit(" "), col("time")), "yyyy-MM-dd HH:mm:ss")) \
#                     .drop("url", "date", "time")

# display(selected_df)

#too many functions increases processing times so simplify each step - 1. add columns, 2. select only required columns while renaming them

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, concat, lit, col

selected_df = df.withColumn("ingestion_date", current_timestamp()) \
                    .withColumn("race_timestamp", to_timestamp(concat(col("date"), lit(" "), col("time")), "yyyy-MM-dd HH:mm:ss")) \
                    .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

final_df = selected_df.select(col("raceId").alias("race_id"), col("year").alias("race_year"), col("date"), col("round"), 
                              col("circuitId").alias("circuit_id"), col("name"), col("race_timestamp"), col("ingestion_date"))


# COMMAND ----------

# MAGIC %md
# MAGIC ###Write the output to processed container in parquet format

# COMMAND ----------

final_df.write.mode('overwrite').partitionBy('race_year').parquet(f'{processed_folder_path}/races')

# COMMAND ----------

races_df = spark.read.parquet(f'{processed_folder_path}/races')