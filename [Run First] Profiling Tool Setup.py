# Databricks notebook source
# MAGIC %md ### Create Default Table in HMS

# COMMAND ----------

df = spark.range(1,10)
df.write.format("delta").mode("overwrite").saveAsTable("default.default")
