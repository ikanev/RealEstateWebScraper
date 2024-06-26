# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

bronze_df = spark.read.option("header", True).csv('/mnt/bronze/csv_estate_2')
bronze_df.printSchema()

# COMMAND ----------

def modify_colums_names(df):
    for name in df.schema.names:
        new_name = name.lower().replace(' ', '_')
        df = df.withColumnRenamed(name, new_name)
    
    return df

# COMMAND ----------

def string_col_to_int(df, col_to_change):
    
    return df.withColumn(col_to_change, col(col_to_change).cast(IntegerType()))


def distance_col_to_int(df, col_to_change):
    df = df.withColumn(col_to_change,\
                                when(col(col_to_change).contains(' meters'),  regexp_replace(col_to_change, ' meters','').cast(IntegerType())) \
                                .when(col(col_to_change).contains(' kilometers'), regexp_replace(col_to_change, ' kilometers','').cast(IntegerType())*1000))

    return df.withColumnRenamed(col_to_change, col_to_change + '_in_meters')
 

def price_col_to_double(df, col_to_change):

    df = df.withColumn(col_to_change, regexp_replace(col_to_change, "[ ]","")) \
                    .withColumn(col_to_change, regexp_extract(col_to_change, r'(\d+(\.\d+)?)', 1) \
                    .cast(DoubleType()))
    
    return df.withColumnRenamed(col_to_change, col_to_change + '_in_euro')


def sq_m_col_to_int(df, col_to_change):

    df = df.withColumn(col_to_change, regexp_replace(col_to_change, ' sq.m','').cast(IntegerType())) 

    return df.withColumnRenamed(col_to_change, col_to_change + '_in_sq_m') 


def string_col_to_bool(df, col_to_change) :
    return df.withColumn(col_to_change, col(col_to_change).cast(BooleanType()))


# COMMAND ----------

def prepare_columns_data(df):

    df = string_col_to_int(df, "date_of_construction")
    df = string_col_to_int(df, "floor")
    df = string_col_to_int(df, "number_of_bathrooms")
    df = string_col_to_int(df, "number_of_bedrooms")
    df = string_col_to_int(df, "number_of_storeys")
    df = string_col_to_int(df, "total_number_of_rooms")

    df = distance_col_to_int(df, "to_food_stores")
    df = distance_col_to_int(df, "to_the_beach")
    df = distance_col_to_int(df, "to_a_railroad_station")
    df = distance_col_to_int(df, "to_medical_facilities")
    df = distance_col_to_int(df, "to_the_airport")
    df = distance_col_to_int(df, "to_the_nearest_big_city")
    df = distance_col_to_int(df, "to_the_historical_city_center")

    df = price_col_to_double(df, "price_per_square_meter")
    df = price_col_to_double(df, "sale_price")
    df = price_col_to_double(df, "operating_expenses")
    df = price_col_to_double(df, "total_year_expenses")

    df = sq_m_col_to_int(df, "floor_area")

    df = df.withColumn('min_contract_period', regexp_extract(col("contract_period"), r'(\d+) (\w+) (\d+)', 1).cast(IntegerType()))
    df = df.withColumn('max_contract_period', regexp_extract(col("contract_period"), r'(\d+) (\w+) (\d+)', 3).cast(IntegerType()))
    df = df.drop("contract_period")

    return df

# COMMAND ----------

bronze_df = modify_colums_names(bronze_df)
silver_df = prepare_columns_data(bronze_df)

# COMMAND ----------

silver_df.write.mode("overwrite").parquet('/mnt/silver/parquet_estate')
silver_df.printSchema()

# COMMAND ----------

df1 = spark.read.parquet('/mnt/silver/parquet_estate')
display(df1)

