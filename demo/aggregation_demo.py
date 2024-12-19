# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

results = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

demo_df = results.filter((results.race_year == 2020) | (results.race_year == 2019))

# COMMAND ----------

from pyspark.sql.functions import sum, count, countDistinct, col

# COMMAND ----------

demo_grouped_df = demo_df.groupBy("driver_name", "race_year") \
                            .agg(sum("points").alias("total_points"), countDistinct("race_name").alias("total_races")) \
                            .orderBy((col("total_points").desc()))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, col, rank

driverRankSpec = Window.partitionBy("race_year").orderBy(col("total_points").desc())
demo_grouped_df = demo_grouped_df.withColumn("rank", rank().over(driverRankSpec))
display(demo_grouped_df)