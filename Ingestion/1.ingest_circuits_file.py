# Databricks notebook source
# MAGIC %md
# MAGIC ### 1. Import circuits csv file

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p-data-source", "")
v_data_source = dbutils.widgets.get("p-data-source")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

circuits_schema = StructType(fields = [StructField("circuitId", IntegerType(), False), 
                                      StructField("circuitRef", StringType(), True), 
                                      StructField("name", StringType(), True), 
                                      StructField("location", StringType(), True), 
                                      StructField("country", StringType(), True), 
                                      StructField("lat", DoubleType(), True), 
                                      StructField("lng", DoubleType(), True), 
                                      StructField("alt", IntegerType(), True), 
                                      StructField("url", StringType(), True)]
                             )

# COMMAND ----------

circuits_df = spark.read.csv(f"{raw_folder_path}/circuits.csv", schema = circuits_schema, header=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Select only required columns

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuits_selected_df = circuits_df.select(col('circuitId'), col('circuitRef'), col('name'), col('location'), col('country'), col('lat'), col('lng'), col('alt'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Rename the column names

# COMMAND ----------

from pyspark.sql.functions import lit

circuits_renamed_df = circuits_selected_df.withColumnRenamed('circuitId', 'circuit_id') \
  .withColumnRenamed('circuitRef', 'circuit_ref') \
  .withColumnRenamed('lng', 'longitude') \
  .withColumnRenamed('lat', 'latitude') \
  .withColumnRenamed('alt', 'altitude') \
  .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.Add ingestion date as a column

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.Write data to datalake as parquet

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

