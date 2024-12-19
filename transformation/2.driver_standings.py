# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(df.limit(10))

# COMMAND ----------

from pyspark.sql.functions import sum, col, count, when

driver_standings_df = df.filter(col("race_year") == 2020) \
                        .groupBy("race_year", "driver_name","driver_nationality", "team") \
                        .agg(sum("points").alias("total_points"), 
                             count(when(col("position") == 1, True)).alias("wins")
                             )

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col

driver_standings_Spec = Window.partitionBy("race_year").orderBy(col("total_points").desc())
driver_standings = driver_standings_df.withColumn("rank", rank().over(driver_standings_Spec))

# COMMAND ----------

display(driver_standings)

# COMMAND ----------

driver_standings.write.mode("overwrite").parquet(f"{presentation_folder_path}/driver_standings")