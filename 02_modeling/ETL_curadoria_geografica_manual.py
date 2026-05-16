# Databricks notebook source
# MAGIC %md
# MAGIC # ETL_curadoria_geografica_manual
# MAGIC
# MAGIC This notebook creates a manually curated geographic reference table for Araçatuba neighborhoods.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup
# MAGIC
# MAGIC Define target table names and helper functions.

# COMMAND ----------

from pyspark.sql.functions import col, lower, trim, regexp_replace, count as spark_count
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
)

CATALOG = "workspace"
SCHEMA = "default"
TABLE_PREFIX = f"{CATALOG}.{SCHEMA}"

MANUAL_GEOCODING_TABLE = f"{TABLE_PREFIX}.ref_bairro_geocoding_manual"
MANUAL_REVIEW_QUEUE_TABLE = f"{TABLE_PREFIX}.ref_bairro_geocoding_manual_review_queue"
VW_CASOS_ESPACIAIS_GEO = f"{TABLE_PREFIX}.vw_casos_espaciais_geo"

print("Configuration loaded.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Manual geographic reference rows
# MAGIC
# MAGIC These records were manually curated as approximate reference points.
# MAGIC
# MAGIC Important: these are not official polygons or official centroids. They are point references intended to improve map coverage while preserving spatial quality labels.

# COMMAND ----------

manual_geo_rows = [
    (
        "Conjunto Habitacional Hilda Mandarino",
        -21.2150,
        -50.4051,
        "manual_reference_point",
        "Approximate point based on neighborhood/logradouro reference. Use as map reference, not official centroid.",
        "medium",
    ),
    (
        "Morada dos Nobres",
        -21.2304,
        -50.4655,
        "manual_reference_point",
        "Approximate point based on neighborhood/logradouro reference. Use as map reference, not official centroid.",
        "medium",
    ),
    (
        "Novo Paraíso",
        -21.1965,
        -50.4520,
        "manual_reference_point",
        "Approximate point based on neighborhood/logradouro reference. Use as map reference, not official centroid.",
        "medium",
    ),
    (
        "Conjunto Habitacional Etheocle Turrini",
        -21.1698,
        -50.4500,
        "manual_reference_point",
        "Approximate point based on neighborhood/logradouro reference. Use as map reference, not official centroid.",
        "medium",
    ),
    (
        "Residencial Jardim Atlântico",
        -21.1535,
        -50.4546,
        "manual_reference_point",
        "Approximate point based on neighborhood/logradouro reference. Review recommended because sources may show divergent points.",
        "medium",
    ),
    (
        "Jardim Aclimação",
        -21.2038,
        -50.4572,
        "manual_reference_point",
        "Approximate point based on neighborhood/logradouro reference. Use as map reference, not official centroid.",
        "medium",
    ),
    (
        "Conjunto Habitacional Castelo Branco",
        -21.1908,
        -50.4308,
        "manual_reference_point",
        "Approximate point based on neighborhood/logradouro reference. Use as map reference, not official centroid.",
        "medium",
    ),
    (
        "Conjunto Habitacional Nossa Senhora Aparecida",
        -21.1879,
        -50.4312,
        "manual_reference_point",
        "Approximate point based on neighborhood/logradouro reference. Use as map reference, not official centroid.",
        "medium",
    ),
    (
        "Jardim do Prado",
        -21.2136,
        -50.4641,
        "manual_reference_point",
        "Approximate point based on neighborhood/logradouro reference. Use as map reference, not official centroid.",
        "medium",
    ),
    (
        "Jussara",
        -21.2268,
        -50.4561,
        "manual_reference_point",
        "Approximate point based on neighborhood/logradouro reference. Use as map reference, not official centroid.",
        "medium",
    ),
    (
        "Jardim América",
        -21.2085,
        -50.4563,
        "manual_reference_point",
        "Approximate point based on neighborhood/logradouro reference. Use as map reference, not official centroid.",
        "medium",
    ),
    (
        "Vila Alba",
        -21.1790,
        -50.4524,
        "manual_reference_point",
        "Approximate point based on neighborhood/logradouro reference. Use as map reference, not official centroid.",
        "medium",
    ),
]

schema_manual_geo = StructType([
    StructField("bairro_padronizado", StringType(), False),
    StructField("manual_latitude", DoubleType(), False),
    StructField("manual_longitude", DoubleType(), False),
    StructField("manual_geocode_source", StringType(), False),
    StructField("manual_geocode_note", StringType(), True),
    StructField("manual_confidence", StringType(), False),
])

df_manual_geo = spark.createDataFrame(manual_geo_rows, schema=schema_manual_geo)

# Compatibility key used by ETL_dim_bairro_geografica.
df_manual_geo = (
    df_manual_geo
    .withColumn("bairro_padronizado_key", lower(trim(col("bairro_padronizado"))))
)

print(f"Manual reference rows: {df_manual_geo.count()}")
display(df_manual_geo.orderBy("bairro_padronizado"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Save manual reference table
# MAGIC
# MAGIC This table will be consumed by `ETL_dim_bairro_geografica`.

# COMMAND ----------

df_manual_geo.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(MANUAL_GEOCODING_TABLE)

print(f"Manual geocoding table saved: {MANUAL_GEOCODING_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Optional review queue
# MAGIC
# MAGIC If `vw_casos_espaciais_geo` already exists, this block creates a review queue with remaining non-mappable neighborhoods.
# MAGIC
# MAGIC This does not block the pipeline. It is only a quality-improvement table.

# COMMAND ----------

if spark.catalog.tableExists(VW_CASOS_ESPACIAIS_GEO):
    df_review_queue = (
        spark.table(VW_CASOS_ESPACIAIS_GEO)
        .filter(col("is_mappable") == False)
        .groupBy(
            col("bairro").alias("bairro_original"),
            "bairro_padronizado",
            "spatial_status",
            "spatial_quality",
        )
        .agg(spark_count("*").alias("cases"))
        .orderBy(col("cases").desc(), col("bairro_original"))
    )

    df_review_queue.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(MANUAL_REVIEW_QUEUE_TABLE)

    print(f"Manual review queue saved: {MANUAL_REVIEW_QUEUE_TABLE}")
    display(df_review_queue)
else:
    print(f"View not found yet: {VW_CASOS_ESPACIAIS_GEO}")
    print("Skipping review queue. Run ETL_dim_bairro_geografica and ETL_validacao_espacial later.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Final check

# COMMAND ----------

print("Manual curation completed.")
print(f"Output table: {MANUAL_GEOCODING_TABLE}")
