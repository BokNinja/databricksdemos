# Databricks notebook source
# Read data from the table 'nyctaxi' into a dataframe
df = spark.read.table("bokbricks.bronze.nyctaxi")
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
spark_df = spark.read.table("bokbricks.bronze.nyctaxi")
df = spark_df.toPandas()
profile = ProfileReport(df, title="Profiling Report")


# COMMAND ----------

profile.to_widgets()

# COMMAND ----------

profile.to_notebook_iframe()
