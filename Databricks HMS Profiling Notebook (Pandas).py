# Databricks notebook source
# MAGIC %md ### Databricks Table Profiling Tool
# MAGIC 
# MAGIC The following tool can be used to profile Data Lake tables registered to the Databricks Hive Metastore.

# COMMAND ----------

# MAGIC %md #### Press the `Run all` to Execute Profiling Tool.
# MAGIC 
# MAGIC Please do not modify the code below.

# COMMAND ----------

databases = [db.databaseName for db in spark.sql("show databases").collect()]
tables = [
    f"{row['database']}.{row['tableName']}"  # <schema>.<table> format
    for db_rows in [spark.sql(f"show tables in {db}").collect() for db in databases]
    for row in db_rows
]

# Table Input
dbutils.widgets.dropdown("Data Lake Table Asset", "default.lineitems", tables)

# Profiling Settings
dbutils.widgets.dropdown(
    "profiler_mode", "Minimal", ["Minimal", "Detailed", "Time Series"], "Data Profiler Mode"
)

table_path = dbutils.widgets.get("Data Lake Table Asset")
profiler_mode = dbutils.widgets.get("profiler_mode")
ts_input = dbutils.widgets.get("ts_mode")

# COMMAND ----------

# Helper Functions
def str_to_bool(s):
    if s == 'True':
         return True
    elif s == 'False':
         return False
      
def convert_date_columns(df):
  date_columns = []
  for column_name, colmun_type in df.dtypes:
    if colmun_type == "date":
      date_columns.append(column_name)
  return date_columns

# COMMAND ----------

import pandas as pd


# Read data from table
df_spark = df = spark.read.table(table_path)

# Convert Spark date columns to Pandas datetime
date_columns = convert_date_columns(df_spark)
df_pd = df_spark.toPandas()
df_pd[date_columns] = df_pd[date_columns].apply(pd.to_datetime, errors="coerce")

# COMMAND ----------

from pandas_profiling import ProfileReport

df_profile = ProfileReport(
    df_pd,
    minimal=False,
    html={'style': 
          {'full_width': True,
           'navbar_show': True,
           'theme':'flatly'}
        }, 
    tsmode= True,
    sortby = "date",
    title=f"Profiling Report : {table_path}",
    progress_bar=True,
    infer_dtypes=False,
)
profile_html = df_profile.to_html()

displayHTML(profile_html)
