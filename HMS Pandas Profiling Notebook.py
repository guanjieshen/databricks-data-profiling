# Databricks notebook source
# MAGIC %md ## Databricks Table Profiling Tool
# MAGIC 
# MAGIC The following tool can be used to profile Data Lake tables registered to the Databricks Hive Metastore.

# COMMAND ----------

# MAGIC %md ### Press the `Run all` to Execute Profiling Tool.
# MAGIC 
# MAGIC Please do not modify the code below.

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
dbutils.widgets.text("num_sample", "1","Data Sample Ratio")

table_path = dbutils.widgets.get("Data Lake Table Asset")
sample_ratio = dbutils.widgets.get("num_sample")

# Read data from table
df_spark = spark.read.table(table_path)
df_spark_sampled = df_spark.sample(fraction=float(sample_ratio))

# Print Basic Metrics
count_original = df_spark.count()
count_sampled = df_spark_sampled.count()
profiler_mode = dbutils.widgets.get("profiler_mode")

converted_spark_df = (df_spark_sampled
                .transform(convert_columns_decimal_to_long)
                .transform(convert_columns_date_to_timestamp_columns))

# # Convert Spark date columns to Pandas datetime
df_pd = converted_spark_df.toPandas()

# COMMAND ----------

# MAGIC  %md #### Profiling Details & Sample Data (first 1000 rows)

# COMMAND ----------

print(f"Selected Table: {table_path}")
print(f"Selected Data Sample Ratio: {sample_ratio}")
print(f"Selected Data Profile Mode: {profiler_mode}\n")


print(f"Number of records before sampling: {count_original}")
print(f"Number of records after sampling: {count_sampled}\n")
display(df_spark_sampled)

# COMMAND ----------

# Print Basic Metrics
count_original = df_spark.count()
count_sampled = df_spark_sampled.count()
profiler_mode = dbutils.widgets.get("profiler_mode")
print(f"Selected Table: {table_path}")
print(f"Selected Data Sample Ratio: {sample_ratio}")
print(f"Selected Data Profile Mode: {profiler_mode}\n")


print(f"Number of records before sampling: {count_original}")
print(f"Number of records after sampling: {count_sampled}")

# COMMAND ----------

# MAGIC  %md #### Profiling Results

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