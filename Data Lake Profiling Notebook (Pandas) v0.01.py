# Databricks notebook source
# MAGIC %md ### Databricks Table Profiling Tool
# MAGIC 
# MAGIC The following tool can be used to profile Data Lake tables registered to the Databricks Hive Metastore.

# COMMAND ----------

# MAGIC %md #### Press the `Run all` to Execute Profiling Tool.
# MAGIC 
# MAGIC Please do not modify the code below.

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

from pyspark.sql.functions import to_timestamp

# Helper Functions
def str_to_bool(s):
    if s == "True":
        return True
    elif s == "False":
        return False


def convert_columns_date_to_timestamp_columns(df):
    for column_name, colmun_type in df.dtypes:
        if colmun_type == "date":
            df = df.withColumn(
                column_name, to_timestamp(df[column_name], "yyyy-MM-dd")
            )
    return df


def convert_columns_decimal_to_long(df):
    for column_name, colmun_type in df.dtypes:
        if "decimal" in colmun_type:
            df = df.withColumn(column_name, df[column_name].cast("double"))
    return df

# COMMAND ----------

import pandas as pd

# Data Lake Location
dbutils.widgets.text("storage_account", "guanjiestorage", "Data Lake Storage Account")
dbutils.widgets.text("container", "datasets","Data Lake Storage Container")
dbutils.widgets.text("directory", "datasets/csv/avacado", "Data Lake Storage Directory")
dbutils.widgets.dropdown("data_type", "CSV", ["CSV", "DELTA", "PARQUET", "JSON"], "Data Type")

# CSV Settings
dbutils.widgets.dropdown("header", "True", ["True","False"], "Does CSV have header?")

# Profiling Settings
dbutils.widgets.dropdown(
    "profiler_mode", "Minimal", ["Minimal", "Detailed", "Time Series"], "Data Profiler Mode"
)
dbutils.widgets.text("num_sample", "1","Data Sample Ratio")

# COMMAND ----------

storage_account = dbutils.widgets.get("storage_account")
container = dbutils.widgets.get("container")
directory = dbutils.widgets.get("directory")
data_type = dbutils.widgets.get("data_type")

table_path = dbutils.widgets.get("Data Lake Table Asset")
sample_ratio = dbutils.widgets.get("num_sample")

storage_account_url = f"abfss://{container}@{storage_account}.dfs.core.windows.net/{directory}"

df = None

if data_type == "CSV":
  df_spark = spark.read.format("csv").options.load(storage_account_url)

if data_type == "DELTA":
  df_spark = spark.read.format("delta").load(storage_account_url)
  
if data_type == "PARQUET":
  df_spark = spark.read.format("parquet").load(storage_account_url)

if data_type == "JSON":
  df_spark = spark.read.format("delta").load(storage_account_url)
  
# Downsample dataset
df_spark_sampled = df_spark.sample(fraction=float(sample_ratio))


converted_spark_df = (df_spark_sampled
                .transform(convert_columns_decimal_to_long)
                .transform(convert_columns_date_to_timestamp_columns))

# # Convert Spark date columns to Pandas datetime
df_pd = converted_spark_df.toPandas()

# Print Basic Metrics
count_original = df_spark.count()
count_sampled = df_spark_sampled.count()
profiler_mode = dbutils.widgets.get("profiler_mode")



# COMMAND ----------

print(f"""
Storage Account: {storage_account}
Storage Container: {container}
Storage Directory: {directory}
Data Type: {data_type}

Selected Data Sample Ratio: {sample_ratio}
Selected Data Profile Mode: {profiler_mode}

Number of records before sampling: {count_original}
Number of records after sampling: {count_sampled}

""")

# COMMAND ----------

from pandas_profiling import ProfileReport

profiler_mode = dbutils.widgets.get("profiler_mode")

df_profile = None

if profiler_mode =="Minimal":
  df_profile = ProfileReport(
      df_pd,
      minimal = True,
      html={'style': 
            {'full_width': True,
             'navbar_show': True,
             'theme':'flatly'}
          }, 
      title=f"Profiling Report : {table_path}",
      progress_bar=True,
      infer_dtypes=False,
  )
  
if profiler_mode =="Detailed":
  df_profile = ProfileReport(
      df_pd,
      minimal = False,
      html={'style': 
            {'full_width': True,
             'navbar_show': True,
             'theme':'flatly'}
          }, 
      title=f"Profiling Report : {table_path}",
      progress_bar=True,
      infer_dtypes=False,
  )
  
if profiler_mode =="Time Series":
  df_profile = ProfileReport(
      df_pd,
      tsmode=True,
      html={'style': 
            {'full_width': True,
             'navbar_show': True,
             'theme':'flatly'}
          }, 
      title=f"Profiling Report : {table_path}",
      progress_bar=True,
      infer_dtypes=False,
  )
  
profile_html = df_profile.to_html()

displayHTML(profile_html)
