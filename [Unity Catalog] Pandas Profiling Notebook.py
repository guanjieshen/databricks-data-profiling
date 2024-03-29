# Databricks notebook source
# MAGIC %pip install ydata-profiling==4.4.0

# COMMAND ----------

df = spark.read.table("gshen_catalog.customers.customer_data")

# COMMAND ----------

from ydata_profiling import ProfileReport

report = ProfileReport(df,
                title='gshen_catalog.customers.customer_data',
                infer_dtypes=False,
                interactions=None,
                missing_diagrams=None,
                correlations={"auto": {"calculate": False},
                              "pearson": {"calculate": True},
                              "spearman": {"calculate": True}})

# COMMAND ----------

dict_plot = report.summarizer
print(dict_plot)

# COMMAND ----------

report_html = report.to_html()
displayHTML(report_html)

# COMMAND ----------

# MAGIC %md ### Databricks Table Profiling Tool
# MAGIC
# MAGIC The following tool can be used to profile Data Lake tables registered to the Databricks Unity Catalog Metastore.

# COMMAND ----------

# MAGIC %md #### Do not modify the code below.

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
