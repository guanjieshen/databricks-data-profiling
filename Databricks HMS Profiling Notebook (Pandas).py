# Databricks notebook source
# MAGIC %md ### Databricks Table Profiling Tool
# MAGIC 
# MAGIC The following tool can be used to profile Data Lake tables registered to the Databricks Hive Metastore.

# COMMAND ----------

# MAGIC %md #### Run Cell #3 to refresh the list of tables.
# MAGIC 
# MAGIC Please do not modify the code below.

# COMMAND ----------

from pandas_profiling import ProfileReport

databases = [
    db.databaseName 
    for db in spark.sql('show databases').collect()
]
tables = [
    f"{row['database']}.{row['tableName']}" #<schema>.<table> format
    for db_rows in [
        spark.sql(f'show tables in {db}').collect() for db in databases
    ] 
    for row in db_rows
]

dbutils.widgets.dropdown("Data Lake Table Asset", "default.lineitems", tables)

table_path = dbutils.widgets.get("Data Lake Table Asset")

df=spark.read.table(table_path).toPandas()
df_profile = ProfileReport(df, minimal=True, title="Profiling Report", progress_bar=False, infer_dtypes=False)
profile_html = df_profile.to_html()

displayHTML(profile_html)
