# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 01 — GitHub to Databricks: a complete Bronze → Silver → Gold pipeline
# MAGIC
# MAGIC **Learning goals:** discover Unity Catalog, create schemas and a volume, ingest nested JSON, flatten and clean with PySpark, aggregate into Gold Delta tables, inspect quality checks, and commit changes through a Databricks Git folder.
# MAGIC
# MAGIC This lab starts from a **fictional LongLaMP-style sample** (not the LongLaMP benchmark). No outbound downloads, API keys, or package installs are needed.
# MAGIC
# MAGIC Run each cell top-to-bottom using **Run cell** (or **Run all** after reading the instructions). Choose **Serverless** compute when prompted.

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Inspect your workspace
# MAGIC
# MAGIC In the left sidebar: **Workspace** contains notebooks and Git folders; **Catalog** contains tables, schemas, and volumes. GitHub stores your source code; Databricks executes the code and stores tables.
# MAGIC
# MAGIC This notebook uses the `workspace` catalog if available; otherwise it uses the current Unity Catalog catalog. We intentionally **do not CREATE CATALOG**, which may be disallowed in Free Edition.

# COMMAND ----------
import json
import re
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

available_catalogs = [r[0] for r in spark.sql("SHOW CATALOGS").collect()]
current_catalog = spark.sql("SELECT current_catalog()").first()[0]
catalog_name = "workspace" if "workspace" in available_catalogs else current_catalog
if catalog_name == "hive_metastore":
    raise ValueError("Select a Unity Catalog catalog in Catalog Explorer, then update catalog_name.")

user_email = spark.sql("SELECT current_user()").first()[0]
user_slug = re.sub(r"[^a-z0-9_]", "_", user_email.split("@")[0].lower())[:24]
namespace = f"capstone_{user_slug}"
bronze_schema = f"{namespace}_bronze"
silver_schema = f"{namespace}_silver"
gold_schema = f"{namespace}_gold"

# Catalog and schema names are validated before interpolating them into SQL.
for identifier in (catalog_name, bronze_schema, silver_schema, gold_schema):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", identifier):
        raise ValueError(f"Unsupported identifier: {identifier!r}; choose a simple catalog name.")

print("Signed in as:", user_email)
print("Unity Catalog catalog:", catalog_name)
print("Available catalogs:", available_catalogs)
print("Your schema prefix:", namespace)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Create the Bronze, Silver, and Gold schemas and a raw file volume
# MAGIC
# MAGIC A **catalog** contains **schemas**; a schema contains Delta **tables** and **volumes**. A volume stores files, not rows in a table. Using a user-specific schema prefix prevents classmates from overwriting one another.

# COMMAND ----------
for schema_name in (bronze_schema, silver_schema, gold_schema):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")

volume_name = "raw_input"
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog_name}.{bronze_schema}.{volume_name}")

volume_file = f"/Volumes/{catalog_name}/{bronze_schema}/{volume_name}/sample_longlamp_style.jsonl"
bronze_table = f"{catalog_name}.{bronze_schema}.publication_requests"
silver_table = f"{catalog_name}.{silver_schema}.publications_clean"
gold_table = f"{catalog_name}.{gold_schema}.publications_by_topic_year"
print("Raw file will be written to:", volume_file)
print("Bronze table:", bronze_table)
print("Silver table:", silver_table)
print("Gold table:", gold_table)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Seed the volume with offline-friendly practice data
# MAGIC
# MAGIC Each `request_id` has a nested `profile` array of publications. The fixture has **8 requests / 28 profiles**, including **2 duplicate paper IDs**, **1 missing title**, and **1 invalid year**. The same data is included as `data/sample_longlamp_style.jsonl` in GitHub.
# MAGIC
# MAGIC The sample is embedded below so this notebook works without Git-folder path assumptions or Hugging Face network access.

# COMMAND ----------
SAMPLE_REQUESTS = [
  {
    "request_id": "request-001",
    "profile": [
      {
        "id": "pub-001",
        "title": "Reliable Evaluation of Small Language Models",
        "abstract": "This fictional teaching example investigates machine learning using a controlled dataset, repeatable evaluation, and a transparent baseline. Its purpose is to illustrate data engineering transformations rather than report research findings.",
        "year": "2018",
        "topic": "machine learning"
      },
      {
        "id": "pub-002",
        "title": "Spatiotemporal Embeddings for City Data",
        "abstract": "This fictional teaching example investigates geospatial ai using a controlled dataset, repeatable evaluation, and a transparent baseline. Its purpose is to illustrate data engineering transformations rather than report research findings.",
        "year": "2019",
        "topic": "geospatial ai"
      },
      {
        "id": "pub-003",
        "title": "Columnar Pipelines at Small Scale",
        "abstract": "This fictional teaching example investigates data engineering using a controlled dataset, repeatable evaluation, and a transparent baseline. Its purpose is to illustrate data engineering transformations rather than report research findings.",
        "year": "2020",
        "topic": "data engineering"
      }
    ]
  },
  {
    "request_id": "request-002",
    "profile": [
      {
        "id": "pub-004",
        "title": "Map Grounded Route Understanding",
        "abstract": "This fictional teaching example investigates geospatial ai using a controlled dataset, repeatable evaluation, and a transparent baseline. Its purpose is to illustrate data engineering transformations rather than report research findings.",
        "year": "2019",
        "topic": "geospatial ai"
      },
      {
        "id": "pub-005",
        "title": "Incremental Aggregation for Data Products",
        "abstract": "This fictional teaching example investigates data engineering using a controlled dataset, repeatable evaluation, and a transparent baseline. Its purpose is to illustrate data engineering transformations rather than report research findings.",
        "year": "2020",
        "topic": "data engineering"
      },
      {
        "id": "pub-006",
        "title": "Object Detection in Harsh Weather",
        "abstract": "This fictional teaching example investigates computer vision using a controlled dataset, repeatable evaluation, and a transparent baseline. Its purpose is to illustrate data engineering transformations rather than report research findings.",
        "year": "2021",
        "topic": "computer vision"
      }
    ]
  },
  {
    "request_id": "request-003",
    "profile": [
      {
        "id": "pub-007",
        "title": "Tracking Data Lineage in ETL",
        "abstract": "This fictional teaching example investigates data engineering using a controlled dataset, repeatable evaluation, and a transparent baseline. Its purpose is to illustrate data engineering transformations rather than report research findings.",
        "year": "2020",
        "topic": "data engineering"
      },
      {
        "id": "pub-008",
        "title": "Lane Marking Segmentation from Cameras",
        "abstract": "This fictional teaching example investigates computer vision using a controlled dataset, repeatable evaluation, and a transparent baseline. Its purpose is to illustrate data engineering transformations rather than report research findings.",
        "year": "2021",
        "topic": "computer vision"
      },
      {
        "id": "pub-009",
        "title": "Weak Supervision for Label Efficient Learning",
        "abstract": "This fictional teaching example investigates machine learning using a controlled dataset, repeatable evaluation, and a transparent baseline. Its purpose is to illustrate data engineering transformations rather than report research findings.",
        "year": "2022",
        "topic": "machine learning"
      },
      {
        "id": "pub-001",
        "title": "Reliable Evaluation of Small Language Models",
        "abstract": "This fictional teaching example investigates machine learning using a controlled dataset, repeatable evaluation, and a transparent baseline. Its purpose is to illustrate data engineering transformations rather than report research findings.",
        "year": "2018",
        "topic": "machine learning"
      }
    ]
  },
  {
    "request_id": "request-004",
    "profile": [
      {
        "id": "pub-010",
        "title": "Multi View Representation Learning",
        "abstract": "This fictional teaching example investigates computer vision using a controlled dataset, repeatable evaluation, and a transparent baseline. Its purpose is to illustrate data engineering transformations rather than report research findings.",
        "year": "2021",
        "topic": "computer vision"
      },
      {
        "id": "pub-011",
        "title": "Model Monitoring Across Changing Data",
        "abstract": "This fictional teaching example investigates machine learning using a controlled dataset, repeatable evaluation, and a transparent baseline. Its purpose is to illustrate data engineering transformations rather than report research findings.",
        "year": "2022",
        "topic": "machine learning"
      },
      {
        "id": "pub-012",
        "title": "Geospatial Indexing of Traffic Signals",
        "abstract": "This fictional teaching example investigates geospatial ai using a controlled dataset, repeatable evaluation, and a transparent baseline. Its purpose is to illustrate data engineering transformations rather than report research findings.",
        "year": "2023",
        "topic": "geospatial ai"
      }
    ]
  },
  {
    "request_id": "request-005",
    "profile": [
      {
        "id": "pub-013",
        "title": "Reliable Evaluation of Small Language Models",
        "abstract": "This fictional teaching example investigates machine learning using a controlled dataset, repeatable evaluation, and a transparent baseline. Its purpose is to illustrate data engineering transformations rather than report research findings.",
        "year": "2022",
        "topic": "machine learning"
      },
      {
        "id": "pub-014",
        "title": "Spatiotemporal Embeddings for City Data",
        "abstract": "This fictional teaching example investigates geospatial ai using a controlled dataset, repeatable evaluation, and a transparent baseline. Its purpose is to illustrate data engineering transformations rather than report research findings.",
        "year": "2023",
        "topic": "geospatial ai"
      },
      {
        "id": "pub-015",
        "title": "Columnar Pipelines at Small Scale",
        "abstract": "This fictional teaching example investigates data engineering using a controlled dataset, repeatable evaluation, and a transparent baseline. Its purpose is to illustrate data engineering transformations rather than report research findings.",
        "year": "2024",
        "topic": "data engineering"
      },
      {
        "id": "pub-005",
        "title": "Incremental Aggregation for Data Products",
        "abstract": "This fictional teaching example investigates data engineering using a controlled dataset, repeatable evaluation, and a transparent baseline. Its purpose is to illustrate data engineering transformations rather than report research findings.",
        "year": "2020",
        "topic": "data engineering"
      }
    ]
  },
  {
    "request_id": "request-006",
    "profile": [
      {
        "id": "pub-016",
        "title": "Map Grounded Route Understanding",
        "abstract": "This fictional teaching example investigates geospatial ai using a controlled dataset, repeatable evaluation, and a transparent baseline. Its purpose is to illustrate data engineering transformations rather than report research findings.",
        "year": "2023",
        "topic": "geospatial ai"
      },
      {
        "id": "pub-017",
        "title": "Incremental Aggregation for Data Products",
        "abstract": "This fictional teaching example investigates data engineering using a controlled dataset, repeatable evaluation, and a transparent baseline. Its purpose is to illustrate data engineering transformations rather than report research findings.",
        "year": "2024",
        "topic": "data engineering"
      },
      {
        "id": "pub-018",
        "title": "Object Detection in Harsh Weather",
        "abstract": "This fictional teaching example investigates computer vision using a controlled dataset, repeatable evaluation, and a transparent baseline. Its purpose is to illustrate data engineering transformations rather than report research findings.",
        "year": "2018",
        "topic": "computer vision"
      },
      {
        "id": "pub-invalid-title",
        "title": "   ",
        "abstract": "Cannot use this record.",
        "year": "2022",
        "topic": "computer vision"
      }
    ]
  },
  {
    "request_id": "request-007",
    "profile": [
      {
        "id": "pub-019",
        "title": "Tracking Data Lineage in ETL",
        "abstract": "This fictional teaching example investigates data engineering using a controlled dataset, repeatable evaluation, and a transparent baseline. Its purpose is to illustrate data engineering transformations rather than report research findings.",
        "year": "2024",
        "topic": "data engineering"
      },
      {
        "id": "pub-020",
        "title": "Lane Marking Segmentation from Cameras",
        "abstract": "This fictional teaching example investigates computer vision using a controlled dataset, repeatable evaluation, and a transparent baseline. Its purpose is to illustrate data engineering transformations rather than report research findings.",
        "year": "2018",
        "topic": "computer vision"
      },
      {
        "id": "pub-021",
        "title": "Weak Supervision for Label Efficient Learning",
        "abstract": "This fictional teaching example investigates machine learning using a controlled dataset, repeatable evaluation, and a transparent baseline. Its purpose is to illustrate data engineering transformations rather than report research findings.",
        "year": "2019",
        "topic": "machine learning"
      },
      {
        "id": "pub-invalid-year",
        "title": "A Study Without Valid Publication Year",
        "abstract": "Cannot use this record.",
        "year": "unknown",
        "topic": "data engineering"
      }
    ]
  },
  {
    "request_id": "request-008",
    "profile": [
      {
        "id": "pub-022",
        "title": "Multi View Representation Learning",
        "abstract": "This fictional teaching example investigates computer vision using a controlled dataset, repeatable evaluation, and a transparent baseline. Its purpose is to illustrate data engineering transformations rather than report research findings.",
        "year": "2018",
        "topic": "computer vision"
      },
      {
        "id": "pub-023",
        "title": "Model Monitoring Across Changing Data",
        "abstract": "This fictional teaching example investigates machine learning using a controlled dataset, repeatable evaluation, and a transparent baseline. Its purpose is to illustrate data engineering transformations rather than report research findings.",
        "year": "2019",
        "topic": "machine learning"
      },
      {
        "id": "pub-024",
        "title": "Geospatial Indexing of Traffic Signals",
        "abstract": "This fictional teaching example investigates geospatial ai using a controlled dataset, repeatable evaluation, and a transparent baseline. Its purpose is to illustrate data engineering transformations rather than report research findings.",
        "year": "2020",
        "topic": "geospatial ai"
      }
    ]
  }
]

# Re-running replaces only this lab's fixture file.
raw_jsonl = "\n".join(json.dumps(record, ensure_ascii=False) for record in SAMPLE_REQUESTS) + "\n"
dbutils.fs.put(volume_file, raw_jsonl, True)
print(f"Wrote {len(SAMPLE_REQUESTS)} request records to the Unity Catalog volume.")
print("See it under Catalog >", catalog_name, ">", bronze_schema, "> Volumes >", volume_name)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Bronze — ingest the nested JSON unchanged
# MAGIC
# MAGIC Bronze preserves the raw nested structure and appends ingestion metadata. Here we use an explicit schema to avoid accidental type inference from dirty input.

# COMMAND ----------
profile_schema = StructType([
    StructField("id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("abstract", StringType(), True),
    StructField("year", StringType(), True),
    StructField("topic", StringType(), True),
])
input_schema = StructType([
    StructField("request_id", StringType(), True),
    StructField("profile", ArrayType(profile_schema), True),
])
bronze_df = (spark.read.schema(input_schema).json(volume_file)
             .withColumn("ingested_at", F.current_timestamp())
             .withColumn("source_file", F.lit("sample_longlamp_style.jsonl")))
bronze_df.write.format("delta").mode("overwrite").saveAsTable(bronze_table)
print("BRONZE rows (requests):", spark.table(bronze_table).count())
display(spark.table(bronze_table).limit(5))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Silver — explode, standardize, validate, and deduplicate
# MAGIC
# MAGIC One request can contain multiple profile records. `explode` flattens the `profile` array. We trim strings, use `try_cast` for the year (so malformed values become null rather than crashing under ANSI mode), filter invalid rows, and deduplicate by publication ID.
# MAGIC
# MAGIC **Expected:** 28 profile elements → 24 distinct valid publications.

# COMMAND ----------
flattened = (spark.table(bronze_table)
    .select("request_id", F.explode("profile").alias("paper"))
    .select(
        F.col("paper.id").alias("paper_id"),
        F.trim(F.col("paper.title")).alias("title"),
        F.trim(F.col("paper.abstract")).alias("abstract"),
        F.expr("try_cast(paper.year AS INT)").alias("publication_year"),
        F.lower(F.trim(F.col("paper.topic"))).alias("topic")
    ))

silver_df = (flattened
    .filter(F.col("paper_id").isNotNull() & (F.length("paper_id") > 0))
    .filter(F.col("title").isNotNull() & (F.length("title") > 0))
    .filter(F.col("publication_year").between(2000, 2030))
    .withColumn("abstract_length", F.length(F.coalesce(F.col("abstract"), F.lit(""))))
    .dropDuplicates(["paper_id"]))

silver_df.write.format("delta").mode("overwrite").saveAsTable(silver_table)
print("RAW NESTED profiles:", flattened.count())
print("SILVER rows:", spark.table(silver_table).count())
display(spark.table(silver_table).orderBy("publication_year", "paper_id").limit(10))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Gold — a business-ready summary
# MAGIC
# MAGIC Gold provides counts and average abstract length by topic and year, ready for dashboards or a downstream model's monitoring report.

# COMMAND ----------
gold_df = (spark.table(silver_table)
    .groupBy("topic", "publication_year")
    .agg(F.countDistinct("paper_id").alias("publication_count"),
         F.round(F.avg("abstract_length"), 1).alias("avg_abstract_length"))
    .orderBy("topic", "publication_year"))
gold_df.write.format("delta").mode("overwrite").saveAsTable(gold_table)
print("GOLD groups:", spark.table(gold_table).count())
display(spark.table(gold_table).orderBy("topic", "publication_year"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. Data-quality assertions and SQL exploration
# MAGIC
# MAGIC Assertions should fail loudly when the pipeline produces the wrong results. Try modifying data, re-running, and observing whether the checks catch errors.

# COMMAND ----------
bronze_count = spark.table(bronze_table).count()
silver_count = spark.table(silver_table).count()
all_profiles_count = flattened.count()
gold_total = (spark.table(gold_table)
              .agg(F.sum("publication_count").alias("n")).first()["n"])
assert bronze_count == 8, f"Expected 8 Bronze requests, found {bronze_count}"
assert all_profiles_count == 28, f"Expected 28 nested profiles, found {all_profiles_count}"
assert silver_count == 24, f"Expected 24 Silver papers, found {silver_count}"
assert gold_total == silver_count, f"Gold total {gold_total} does not equal Silver {silver_count}"
assert spark.table(silver_table).select("paper_id").distinct().count() == silver_count
assert spark.table(silver_table).filter(F.col("publication_year").isNull()).count() == 0
print("PASS — all quality checks succeeded (8 Bronze requests, 28 profiles, 24 Silver papers).")

# COMMAND ----------
# Display query results with the dynamic Unity Catalog table name.
summary_query = f"""
SELECT topic, SUM(publication_count) AS papers
FROM {gold_table}
GROUP BY topic
ORDER BY papers DESC, topic
"""
display(spark.sql(summary_query))
print("Explore the three schemas in Catalog Explorer and try the SQL Editor.")
print("Suggested SQL query: SELECT * FROM", gold_table, "LIMIT 20;")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 8. Git exercise — commit your changes
# MAGIC
# MAGIC 1. Create a feature branch in your Databricks **Git folder** (not directly on `main`).
# MAGIC 2. Open `notebooks/02_student_challenge.py` and complete the TODOs.
# MAGIC 3. Run the challenge notebook, verify its assertions, and commit your changes with a descriptive message.
# MAGIC 4. Push the branch and open a pull request on GitHub.
# MAGIC
# MAGIC **Evidence for submission:** (a) Bronze/Silver/Gold table names, (b) proof of the PASS message, (c) GitHub PR URL, (d) 2–3 sentences explaining your data-quality choice.
