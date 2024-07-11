# Databricks notebook source
# MAGIC %md
# MAGIC Import necessary packages in the notepade.

# COMMAND ----------

import pyspark
from pyspark.sql.functions import *
from delta import *
from delta.tables import *

# COMMAND ----------

# MAGIC %md
# MAGIC Function that creates Delta Table from for gold layer.
# MAGIC
# MAGIC Here I use 'save' instead of 'saveAsTable' because i don't do any sql transformation in Hive metastore and I don't need table saved in Hive.

# COMMAND ----------

def create_DeltaTable(source: str, destination: str) -> 'DeltaTable':
    df: 'DataFrame' = spark.read.parquet(source)
    df.write.format("delta").option("mergeSchema", "true").mode("overwrite").save(destination)
    deltaTable: 'DeltaTable' = DeltaTable.forPath(spark, destination)

    return deltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC Function that enables cache and add Z-ordering for Delta Table.

# COMMAND ----------

def optimize_deltaTable(deltaTable: 'DeltaTable') -> None:
  spark.conf.set("spark.databricks.io.cache.enabled", "true")
  
  indexed_columns = ["floor_area_in_sq_m", "sale_price_in_euro"]
  deltaTable.optimize().executeZOrderBy(indexed_columns)  

# COMMAND ----------

# MAGIC %md
# MAGIC Creating and optimizing Delta Table for the gold layer.
# MAGIC
# MAGIC Also showing the content of the Delta Table.

# COMMAND ----------

data_source: str = "/mnt/silver/parquet_estate"
deltaTable_destination: str = "/mnt/gold/delta_estate"

deltaTable: 'DeltaTable' = create_DeltaTable(data_source, deltaTable_destination)
optimize_deltaTable(deltaTable)

display(deltaTable.toDF())

# COMMAND ----------

# MAGIC %md
# MAGIC Just for test purposes, saving the Delta Table as CSV file and printing the schema of the CSV file.

# COMMAND ----------

deltaTable.toDF().write.mode('overwrite').option('header',True).csv('/mnt/gold/csv_real_estate')
csv_df =  spark.read.option("header", True).csv('/mnt/gold/csv_real_estate')
csv_df.printSchema()
