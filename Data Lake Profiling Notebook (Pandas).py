# Databricks notebook source
# MAGIC %md ### Data Lake Table Profiling Tool
# MAGIC 
# MAGIC The following tool can be used to profile directly from locations within the Data Lake (Azure Data Lake Storage).

# COMMAND ----------

# MAGIC %md #### Do not modify the code below.

# COMMAND ----------

# MAGIC %md ###### Input Details

# COMMAND ----------

# Data Lake Location
dbutils.widgets.text("storage_account", "storageaccount", "Data Lake Storage Account")
dbutils.widgets.text("container", "container","Data Lake Storage Container")
dbutils.widgets.text("directory", "directory", "Data Lake Storage Directory")
dbutils.widgets.dropdown("data_type", "CSV", ["CSV", "DELTA", "PARQUET", "JSON"], "Data Type")

# Profiling Settings
dbutils.widgets.dropdown("minimal_mode", "True", ["True","False"], "Minimal Mode")

# CSV Settings
dbutils.widgets.dropdown("header", "True", ["True","False"], "Does CSV have header?")


storage_account = dbutils.widgets.get("storage_account")
container = dbutils.widgets.get("container")
directory = dbutils.widgets.get("directory")
data_type = dbutils.widgets.get("data_type")

print(f"""
Storage Account: {storage_account}
Storage Container: {container}
Storage Directory: {directory}
Data Type: {data_type}
""")


# COMMAND ----------

from pandas_profiling import ProfileReport


storage_account = dbutils.widgets.get("storage_account")

storage_account_url = f"abfss://{container}@{storage_account}.dfs.core.windows.net/{directory}"

df = None

if data_type == "CSV":
  f = spark.read.format("csv").options.load(storage_account_url)

if data_type == "DELTA":
  df = spark.read.format("delta").load(storage_account_url)
  
if data_type == "PARQUET":
  df = spark.read.format("parquet").load(storage_account_url)

if data_type == "JSON":
  df = spark.read.format("delta").load(storage_account_url)


df_profile = ProfileReport(df.toPandas(), minimal=True, title="Profiling Report", progress_bar=True, infer_dtypes=False)
profile_html = df_profile.to_html()

displayHTML(profile_html)

# COMMAND ----------


