# Databricks notebook source
# Read data from the table 'nyctaxi' into a dataframe
df = spark.read.table("bokbricks.dlt_wh_uc_py.sales_orders_raw")
display(df)

# COMMAND ----------

dbutils.data.summarize(df)

# COMMAND ----------

# MAGIC %pip install --upgrade typing-extensions
# MAGIC %pip install ydata-profiling
# MAGIC dbutils.library.restartPython()
# MAGIC

# COMMAND ----------

from ydata_profiling import ProfileReport
spark_df = spark.read.table("bokbricks.dlt_wh_uc_py.sales_orders_raw")
df = spark_df.toPandas()
profile = ProfileReport(df, title="Profiling Report")


# COMMAND ----------

profile.to_widgets()
