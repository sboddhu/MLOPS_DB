# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 02 — Git change, data quality, and a new Gold product
# MAGIC
# MAGIC **Goal:** create an additional Gold table that summarizes all publications by topic (across years), then publish the code change through GitHub.
# MAGIC
# MAGIC Start on a feature branch created inside your Databricks Git folder. Run Lab 01 at least once before this notebook.
# MAGIC
# MAGIC **Task A:** Complete the two TODO sections below.
# MAGIC **Task B:** Verify that the new table totals 24 publications.
# MAGIC **Task C:** Commit, push, create a PR, and describe your changes.

# COMMAND ----------
import re
from pyspark.sql import functions as F
catalogs = [r[0] for r in spark.sql("SHOW CATALOGS").collect()]
catalog = "workspace" if "workspace" in catalogs else spark.sql("SELECT current_catalog()").first()[0]
user = spark.sql("SELECT current_user()").first()[0]
slug = re.sub(r"[^a-z0-9_]", "_", user.split("@")[0].lower())[:24]
source_table = f"{catalog}.capstone_{slug}_silver.publications_clean"
target_table = f"{catalog}.capstone_{slug}_gold.publications_by_topic"
print('Input:', source_table)
print('Output:', target_table)

# COMMAND ----------
# TODO 1: Build a DataFrame from the Silver table, grouping by topic.
# Produce two columns: topic, publication_count.
# HINT: spark.table(source_table).groupBy('topic').agg(F.countDistinct(...))
# Replace `None` below with your expression.
by_topic_df = None

# COMMAND ----------
# TODO 2: Persist the new Gold table as Delta, with mode='overwrite'.
# HINT: dataframe.write.format('delta').mode('overwrite').saveAsTable(target_table)
# Delete `pass` and add your write statement.
pass

# COMMAND ----------
# Validation: run this cell after completing both TODOs.
assert by_topic_df is not None, 'Complete TODO 1 first.'
assert spark.table(target_table).agg(F.sum('publication_count')).first()[0] == 24
print('PASS — new Gold topic summary contains 24 publications.')
display(spark.table(target_table).orderBy('topic'))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Reflect and submit
# MAGIC
# MAGIC In your PR description, answer: **Why was the Gold table created from Silver rather than directly from the raw JSON volume?**
# MAGIC
# MAGIC Optional extension: add a `topic_share_pct` column using a window, and include a screenshot showing the sum is approximately 100%.
