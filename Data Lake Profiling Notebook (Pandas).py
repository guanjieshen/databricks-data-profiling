# Databricks notebook source
# MAGIC %md ### Data Lake Table Profiling Tool
# MAGIC 
# MAGIC The following tool can be used to profile directly from locations within the Data Lake (Azure Data Lake Storage).

# COMMAND ----------

# MAGIC %md #### Do not modify the code below.

# COMMAND ----------

dbutils.widgets.text("Data Lake Storage Account", "storageaccount")
dbutils.widgets.text("Data Lake Storage Container", "container")
dbutils.widgets.text("Data Lake Storage Directory", "directory")

dbutils.widgets.dropdown("Data Type", "CSV", ["CSV", "DELTA", "PARQUET", "JSON"])


# COMMAND ----------

from pandas_profiling import ProfileReport

sql_list_of_data_attributes = spark.sql("""
SELECT distinct(concat(table_catalog, '.', table_schema, '.', table_name)) data_asset_name  FROM system.information_schema.tables
where table_schema !='information_schema'
""").collect()

list_of_data_attributes =[]
for attribute in sql_list_of_data_attributes:
  list_of_data_attributes.append(attribute.data_asset_name)

dbutils.widgets.dropdown("Data Lake Table Asset", "gshen_catalog.customers.customer_data", list_of_data_attributes)

table_path = dbutils.widgets.get("Data Lake Table Asset")
df=spark.read.table(table_path).toPandas()

df_profile = ProfileReport(df, minimal=True, title="Profiling Report", progress_bar=False, infer_dtypes=False)
profile_html = df_profile.to_html()

displayHTML(profile_html)
