# Databricks notebook source
import pyspark
from pyspark.sql.functions import *
from delta import *
from delta.tables import *

# COMMAND ----------

# MAGIC %md
# MAGIC Here I use 'save' instead of 'saveAsTable' because i don't do any sql transformation in Hive metastore and I don't need table saved in Hive.

# COMMAND ----------

df = spark.read.parquet('/mnt/silver/parquet_estate')
df.write.format("delta").option("mergeSchema", "true").mode("overwrite").save('/mnt/gold/delta_estate')
deltaTable = DeltaTable.forPath(spark, "/mnt/gold/delta_estate")

# COMMAND ----------

spark.conf.set("spark.databricks.io.cache.enabled", "true")

indexed_columns = ["floor_area_in_sq_m", "sale_price_in_euro"]
deltaTable.optimize().executeZOrderBy(indexed_columns)

# COMMAND ----------

display(deltaTable.toDF())

# COMMAND ----------

deltaTable.toDF().write.mode('overwrite').option('header',True).csv('/mnt/gold/csv_real_estate')
csv_df =  spark.read.option("header", True).csv('/mnt/gold/csv_real_estate')
csv_df.printSchema()
