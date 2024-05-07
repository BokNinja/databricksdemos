# Databricks notebook source
# Read data from the table 'nyctaxi' into a dataframe
df = spark.read.table("bokbricks.dlt_wh_uc_py.sales_orders_cleaned")
display(df)

# COMMAND ----------

dbutils.data.summarize(df)

# COMMAND ----------

# MAGIC %pip install --upgrade typing-extensions
# MAGIC %pip install --upgrade pandas pandas_profiling
# MAGIC %pip install ydata-profiling
# MAGIC dbutils.library.restartPython()
# MAGIC

# COMMAND ----------

from ydata_profiling import ProfileReport
import datetime
import pandas as pd

spark_df = spark.read.table("bokbricks.dlt_wh_uc_py.sales_orders_cleaned")
df = spark_df.toPandas()

# Convert date column to datetime type
df['order_datetime'] = pd.to_datetime(df['order_datetime'])
df['order_date'] = pd.to_datetime(df['order_date'])

profile = ProfileReport(df, title="Profiling Report")


# COMMAND ----------

profile.to_widgets()
