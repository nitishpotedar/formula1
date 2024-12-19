# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

circuits_df = spark.read.parquet("/mnt/formula1dlby63/processed/circuits")
drivers_df = spark.read.parquet("/mnt/formula1dlby63/processed/drivers")
constructors_df = spark.read.parquet("/mnt/formula1dlby63/processed/constructors")
races_df = spark.read.parquet("/mnt/formula1dlby63/processed/races")
results_df = spark.read.parquet("/mnt/formula1dlby63/processed/results")

# COMMAND ----------

results_table = results_df.join(races_df, results_df.race_id == races_df.race_id, "left") \
                        .join(drivers_df, results_df.driver_id == drivers_df.driver_id, "left") \
                        .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id, "left") \
                        .join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "left")

# COMMAND ----------

from pyspark.sql.functions import col

final_table = results_table.select(
    races_df.name.alias("race_name"),
    races_df.race_year.alias("race_year"),
    races_df.date.alias("race_date"),
    circuits_df.location.alias("circuit_location"),
    drivers_df.name.alias("driver_name"),
    drivers_df.number.alias("driver_number"),
    drivers_df.nationality.alias("driver_nationality"),
    constructors_df.constructor_name.alias("team"),
    results_df.grid,
    results_df.fastest_lap_time,
    results_df.time.alias("race_time"),
    results_df.points,
    results_df.position,
)

# COMMAND ----------

final_table.write.mode("overwrite").parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

from pyspark.sql.functions import col

final_table.filter((col("race_year") == 2020) | (col("race_name") == "Azerbaijan Grand Prix"))

# COMMAND ----------

