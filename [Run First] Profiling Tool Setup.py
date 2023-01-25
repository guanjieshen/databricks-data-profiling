# Databricks notebook source
df = spark.range(1,10)
df.write.format("delta").mode("overwrite").saveAsTable("default.default")
