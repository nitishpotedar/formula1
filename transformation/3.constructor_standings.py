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

constructor_standings_df = df.filter(col("race_year") == 2020) \
                        .groupBy("race_year", "team") \
                        .agg(sum("points").alias("total_points"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col

constructor_standings_Spec = Window.partitionBy("race_year").orderBy(col("total_points").desc())
constructor_standings = constructor_standings_df.withColumn("rank", rank().over(constructor_standings_Spec))

# COMMAND ----------

display(constructor_standings)

# COMMAND ----------

constructor_standings.write.mode("overwrite").parquet(f"{presentation_folder_path}/constructor_standings")