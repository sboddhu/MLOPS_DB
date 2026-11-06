# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 00 — Get oriented in Databricks
# MAGIC
# MAGIC **Where to click** (left navigation):
# MAGIC - **Workspace:** your notebook files and cloned Git folders
# MAGIC - **Catalog:** data catalog → schema → tables and volumes
# MAGIC - **SQL Editor:** write SQL queries against Delta tables
# MAGIC - **Compute:** this Free Edition exercise uses **Serverless**, with no classic cluster creation
# MAGIC
# MAGIC **GitHub is not Databricks:** GitHub stores and versions code; Databricks computes over the data and persists Delta tables.
# MAGIC
# MAGIC Click **Run cell** on each code cell below.

# COMMAND ----------
print('Hello from a Databricks notebook!')
print('Signed-in user:', spark.sql('SELECT current_user()').first()[0])
print('Current catalog:', spark.sql('SELECT current_catalog()').first()[0])
print('Visible catalogs:', [r[0] for r in spark.sql('SHOW CATALOGS').collect()])

# COMMAND ----------
# MAGIC %md
# MAGIC ## Try a Spark DataFrame
# MAGIC
# MAGIC A DataFrame looks like a table. Spark executes computations when you call actions such as `count()` or `display()`.

# COMMAND ----------
from pyspark.sql import functions as F
hello = spark.createDataFrame([
    (1, 'bronze'), (2, 'silver'), (3, 'gold')
], ['step', 'layer'])
display(hello.withColumn('label', F.upper('layer')))

# COMMAND ----------
# MAGIC %md
# MAGIC Now open **Catalog** in the sidebar. You will populate Bronze/Silver/Gold in the next notebook.
