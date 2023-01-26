# Databricks notebook source
# MAGIC %md ## Data Lake Profiling Tool
# MAGIC 
# MAGIC The following tool can be used to profile Data Lake tables directly within Azure Data Lake Storage.
# MAGIC 
# MAGIC ### How to use this Notebook
# MAGIC 
# MAGIC 1. Click `View` on the top left of the notebook, and select `Results Only`
# MAGIC 
# MAGIC 1. Select the `Data Profiling Cluster` from the `Cluster dropdown` 
# MAGIC 
# MAGIC 1. Click the `Run all` button on the top right.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ### Input Fields
# MAGIC 
# MAGIC - __[Data Lake Storage Account]:__ Azure Data Lake Storage Account Name.
# MAGIC - __[Data Lake Storage Container]:__ Azure Data Lake Storage Container Name.
# MAGIC - __[Data Lake Storage Directory]:__ Azure Data Lake Storage Container Name.
# MAGIC 
# MAGIC - __[Data Profiler Export Mode]:__ Export modes for profile report
# MAGIC   - `Notebook Visual`: Shows the profile report within the notebook.
# MAGIC 
# MAGIC   - `HTML Download`: Generates a download link for the report.
# MAGIC 
# MAGIC   - `Both`: Shows the profile report within the notebook and generates a download link for the report.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC - __[Data Profiler Mode]__: Level of profiling.
# MAGIC 
# MAGIC   - `Minimal`: Calculates only basic profiling metrics.
# MAGIC 
# MAGIC   - `Detailed`: Calculates detailed profiling metrics. Including correlation and interactions. 
# MAGIC 
# MAGIC   - `Time Series`: Calculates time-series specific statistics such as PACF and ACF plots. Requires a dataset with a single `timestamp` or `date` column.
# MAGIC 
# MAGIC - __[Data Sample Ratio]__: Sampling ratio for profiling. 
# MAGIC 
# MAGIC   - Default is `0.1`, which is a 10% sample of the original dataset.

# COMMAND ----------

# MAGIC %md ### Press the `Run all` to Execute Profiling Tool.
# MAGIC 
# MAGIC Please do not modify the code below.

# COMMAND ----------

from pyspark.sql.functions import to_timestamp

### Helper Functions
# Convert string to bool
def str_to_bool(s):
    if s == "True":
        return True
    elif s == "False":
        return False


# Convert date to timestamp
def convert_columns_date_to_timestamp_columns(df):
    for column_name, colmun_type in df.dtypes:
        if colmun_type == "date":
            df = df.withColumn(column_name, to_timestamp(df[column_name], "yyyy-MM-dd"))
    return df


# Convert decimal to long
def convert_columns_decimal_to_long(df):
    for column_name, colmun_type in df.dtypes:
        if "decimal" in colmun_type:
            df = df.withColumn(column_name, df[column_name].cast("double"))
    return df

# COMMAND ----------

import pandas as pd

# Profiling Settings
dbutils.widgets.dropdown(
    "profiler_mode",
    "Minimal",
    ["Minimal", "Detailed", "Time Series"],
    "Data Profiler Mode",
)
dbutils.widgets.text("num_sample", "0.1", "Data Sample Ratio")
dbutils.widgets.dropdown(
    "export_mode",
    "Notebook Visual",
    ["Notebook Visual", "HTML Download", "Both"],
    "Data Profiler Export Mode",
)

# Data Lake Widgets
dbutils.widgets.text("storage_account", "guanjiestorage", "Data Lake Storage Account")
dbutils.widgets.text("container", "datasets", "Data Lake Storage Container")
dbutils.widgets.text(
    "directory", "profiling/csv/avacado/", "Data Lake Storage Directory"
)
dbutils.widgets.dropdown(
    "data_type", "CSV", ["CSV", "DELTA", "PARQUET", "JSON"], "Data Type"
)


# COMMAND ----------

# Get input values
storage_account = dbutils.widgets.get("storage_account")
container = dbutils.widgets.get("container")
directory = dbutils.widgets.get("directory")
data_type = dbutils.widgets.get("data_type")
sample_ratio = dbutils.widgets.get("num_sample")
export_mode = dbutils.widgets.get("export_mode")
profiler_mode = dbutils.widgets.get("profiler_mode")


# Remove Optional Widgets
try:
  dbutils.widgets.remove("csv_header")
  dbutils.widgets.remove("csv_delimiter")
except:
  pass

try:
  dbutils.widgets.remove("json_multiline")
except:
  pass

# CSV Settings
if data_type == "CSV":
    dbutils.widgets.dropdown("csv_header", "True", ["True", "False"], "CSV Header")
    dbutils.widgets.text("csv_delimiter", ",", "CSV Delimiter?")
    has_header = dbutils.widgets.get("csv_header")
    delimiter = dbutils.widgets.get("csv_delimiter")

# JSON Settings
if data_type == "JSON":
    dbutils.widgets.dropdown(
        "json_multiline", "False", ["True", "False"], "JSON Multi-line"
    )
    multiline = dbutils.widgets.get("multiline")

# COMMAND ----------

storage_account_url = (
    f"abfss://{container}@{storage_account}.dfs.core.windows.net/{directory}"
)

df_spark_reader = None

if data_type == "CSV":
    df_spark_reader = (
        spark.read.format("csv")
        .option("inferSchema", "true")
        .option("header", has_header)
        .option("delimiter", delimiter)
    )

if data_type == "DELTA":
    df_spark_reader = spark.read.format("delta")

if data_type == "PARQUET":
    df_spark_reader = spark.read.format("parquet")

if data_type == "JSON":
    df_spark_reader = spark.read.option("multiline", multiline).format("json")

# Load dataset into Notebook
df_spark = df_spark_reader.load(storage_account_url)

# Downsample dataset
df_spark_sampled = df_spark.sample(fraction=float(sample_ratio))

converted_spark_df = df_spark_sampled.transform(
    convert_columns_decimal_to_long
).transform(convert_columns_date_to_timestamp_columns)

# # Convert Spark date columns to Pandas datetime
df_pd = converted_spark_df.toPandas()

# Print Basic Metrics
count_original = df_spark.count()
count_sampled = df_spark_sampled.count()

# COMMAND ----------

# MAGIC  %md ### Profiling Details & Sample Data (first 1000 rows)

# COMMAND ----------

print(
    f"""
Storage Account: {storage_account}
Storage Container: {container}
Storage Directory: {directory}
Data Type: {data_type}

Selected Data Sample Ratio: {sample_ratio}
Selected Data Profile Mode: {profiler_mode}

Number of records before sampling: {count_original}
Number of records after sampling: {count_sampled}

"""
)

display(df_spark_sampled)

# COMMAND ----------

# MAGIC  %md ### Profiling Results

# COMMAND ----------

from pandas_profiling import ProfileReport



df_profile = None

if profiler_mode == "Minimal":
    df_profile = ProfileReport(
        df_pd,
        minimal=True,
        html={"style": {"full_width": True, "navbar_show": True, "theme": "flatly"}},
        title=f"Profiling Report : {storage_account_url}",
        progress_bar=True,
        infer_dtypes=False,
    )

if profiler_mode == "Detailed":
    df_profile = ProfileReport(
        df_pd,
        minimal=False,
        html={"style": {"full_width": True, "navbar_show": True, "theme": "flatly"}},
        title=f"Profiling Report : {storage_account_url}",
        progress_bar=True,
        infer_dtypes=False,
    )

if profiler_mode == "Time Series":
    df_profile = ProfileReport(
        df_pd,
        tsmode=True,
        html={"style": {"full_width": True, "navbar_show": True, "theme": "flatly"}},
        title=f"Profiling Report : {storage_account_url}",
        progress_bar=True,
        infer_dtypes=False,
    )

# COMMAND ----------

# MAGIC  %md #### Profile Download Link

# COMMAND ----------

from IPython.core.display import display as ip_display, HTML
import uuid


# Export profile to file
if export_mode == "HTML Download" or export_mode == "Both":
    file_name = str(uuid.uuid4())[:8]
    df_profile.to_file(f"/dbfs/FileStore/data_profiles/{file_name}.html")
    workspaceURL = spark.conf.get("spark.databricks.workspaceUrl")
    downloadURL = f"/files/data_profiles/{file_name}.html"
    ip_display(HTML(f"""<a href="{downloadURL}">Download Profiling Report</a>"""))

# COMMAND ----------

# MAGIC  %md #### Notebook Profile Report

# COMMAND ----------

# Show profile in notebook
if export_mode == "Notebook Visual" or export_mode == "Both":
    profile_html = df_profile.to_html()
    displayHTML(profile_html)
