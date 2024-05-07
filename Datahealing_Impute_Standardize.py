# Databricks notebook source
#Numeric Imputation
import pandas as pd
from sklearn.impute import SimpleImputer

# Load the dataset
spark_df = spark.read.table("bokbricks.dlt_wh_uc_py.sales_orders_cleaned")
df = spark_df.toPandas()

# Check for missing values
print(df.isnull().sum())

# Select only numeric columns
numeric_cols = df.select_dtypes(include=['float64', 'int']).columns

# Create an instance of the SimpleImputer class
imputer = SimpleImputer(strategy='mean')

# Impute missing values in numeric columns
df[numeric_cols] = imputer.fit_transform(df[numeric_cols])

# Check if missing values have been imputed
print(df.isnull().sum())

# COMMAND ----------

from sklearn.impute import SimpleImputer
from pyspark.sql.functions import lit, array, col

# Load the dataset
df = spark.table("bokbricks.dlt_wh_uc_py.sales_orders_cleaned")

# Select columns with object dtype
object_cols = [col_name for col_name, col_type in df.dtypes if col_type == "string"]

# Create an instance of the SimpleImputer class with 'most_frequent' strategy
imputer = SimpleImputer(strategy='most_frequent')

# Convert the DataFrame to Pandas for imputation
pandas_df = df.select(object_cols).toPandas()

# Impute missing values in selected columns
imputed_values = imputer.fit_transform(pandas_df)

# Create a new DataFrame with the imputed values
imputed_df = df
for i, col_name in enumerate(object_cols):
    imputed_df = imputed_df.withColumn(col_name, lit(None))
    imputed_df = imputed_df.withColumn(col_name, array([lit(value) for value in imputed_values[:, i]]))

# Check if missing values have been imputed
imputed_df.select([col(c).isNull().alias(c) for c in object_cols]).show()

# COMMAND ----------

from pyspark.sql.functions import col

null_counts = imputed_df.select([col(c).isNull().alias(c) for c in imputed_df.columns])\
                       .groupBy()\
                       .sum()

null_counts.show()
