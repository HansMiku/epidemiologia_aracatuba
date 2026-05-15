# Databricks notebook source
# MAGIC %md
# MAGIC # ETL_dim_bairro_geografica
# MAGIC
# MAGIC This notebook creates the final geographic neighborhood dimension and spatial consumption views.
# MAGIC
# MAGIC This notebook is intentionally conservative: records without reliable neighborhood coordinates are preserved and classified, not silently converted into false map points.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup and input checks

# COMMAND ----------

from pyspark.sql.functions import col, lit, coalesce, when, lower, trim, count as spark_count
from pyspark.sql.types import DoubleType, StringType, IntegerType, BooleanType

CATALOG = "workspace"
SCHEMA = "default"
TABLE_PREFIX = f"{CATALOG}.{SCHEMA}"

DIM_BAIRRO_TABLE = f"{TABLE_PREFIX}.dim_bairro"
STANDARDIZED_CURATED_TABLE = f"{TABLE_PREFIX}.ref_bairro_padronizado_curado"
STANDARDIZED_BASE_TABLE = f"{TABLE_PREFIX}.ref_bairro_padronizado"

GEOCODING_VALIDATED_TABLE = f"{TABLE_PREFIX}.bairro_geocoding_validated"
GEOCODING_DRAFT_TABLE = f"{TABLE_PREFIX}.bairro_geocoding_draft"
MANUAL_GEOCODING_TABLE = f"{TABLE_PREFIX}.ref_bairro_geocoding_manual"

OUTPUT_TABLE = f"{TABLE_PREFIX}.dim_bairro_geografica"

VW_CASOS_ESPACIAIS = f"{TABLE_PREFIX}.vw_casos_espaciais"
VW_CASOS_ESPACIAIS_GEO = f"{TABLE_PREFIX}.vw_casos_espaciais_geo"
VW_MAPA_CASOS_BAIRRO = f"{TABLE_PREFIX}.vw_mapa_casos_bairro"
VW_MAPA_CASOS_BAIRRO_MAPEAVEL = f"{TABLE_PREFIX}.vw_mapa_casos_bairro_mapeavel"
VW_QUALIDADE_ESPACIAL = f"{TABLE_PREFIX}.vw_qualidade_espacial"

# Approximate municipal center. Used only as a plotting fallback.
ARACATUBA_FALLBACK_LAT = -21.2089
ARACATUBA_FALLBACK_LON = -50.4328

if not spark.catalog.tableExists(DIM_BAIRRO_TABLE):
    raise ValueError(f"Missing table: {DIM_BAIRRO_TABLE}. Run ETL_dimensoes first.")

if not spark.catalog.tableExists(VW_CASOS_ESPACIAIS):
    raise ValueError(f"Missing view: {VW_CASOS_ESPACIAIS}. Run ETL_dimensoes first.")

if spark.catalog.tableExists(STANDARDIZED_CURATED_TABLE):
    STANDARDIZED_TABLE = STANDARDIZED_CURATED_TABLE
    print(f"Using curated standardization table: {STANDARDIZED_TABLE}")
elif spark.catalog.tableExists(STANDARDIZED_BASE_TABLE):
    STANDARDIZED_TABLE = STANDARDIZED_BASE_TABLE
    print(f"Using base standardization table: {STANDARDIZED_TABLE}")
else:
    raise ValueError(
        f"Missing standardization table. Run ETL_padroniza_bairros first. "
        f"Expected {STANDARDIZED_CURATED_TABLE} or {STANDARDIZED_BASE_TABLE}."
    )

if spark.catalog.tableExists(GEOCODING_VALIDATED_TABLE):
    GEOCODING_TABLE = GEOCODING_VALIDATED_TABLE
    GEOCODING_SOURCE_MODE = "validated"
    print(f"Using validated geocoding table: {GEOCODING_TABLE}")
elif spark.catalog.tableExists(GEOCODING_DRAFT_TABLE):
    GEOCODING_TABLE = GEOCODING_DRAFT_TABLE
    GEOCODING_SOURCE_MODE = "draft"
    print(f"Using draft geocoding table: {GEOCODING_TABLE}")
else:
    GEOCODING_TABLE = None
    GEOCODING_SOURCE_MODE = "missing"
    print("No geocoding table found. The geographic dimension will be created without real neighborhood coordinates.")

if spark.catalog.tableExists(MANUAL_GEOCODING_TABLE):
    MANUAL_SOURCE_MODE = "available"
    print(f"Using manual geocoding table: {MANUAL_GEOCODING_TABLE}")
else:
    MANUAL_SOURCE_MODE = "missing"
    print("No manual geocoding table found. Continuing with OSM geocoding only.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load input tables

# COMMAND ----------

def add_missing_column(df, column_name, data_type):
    if column_name not in df.columns:
        return df.withColumn(column_name, lit(None).cast(data_type))
    return df

schema_manual_geo_empty = """
    bairro_padronizado STRING,
    manual_latitude DOUBLE,
    manual_longitude DOUBLE,
    manual_geocode_source STRING,
    manual_geocode_note STRING,
    manual_confidence STRING,
    bairro_padronizado_key STRING
"""

df_dim_bairro = spark.table(DIM_BAIRRO_TABLE)
df_standardized = spark.table(STANDARDIZED_TABLE)

df_standardized = add_missing_column(df_standardized, "id_bairro_canonico", IntegerType())
df_standardized = add_missing_column(df_standardized, "bairro_padronizado", StringType())
df_standardized = add_missing_column(df_standardized, "query_geocoding", StringType())
df_standardized = add_missing_column(df_standardized, "match_method", StringType())
df_standardized = add_missing_column(df_standardized, "needs_review", BooleanType())
df_standardized = add_missing_column(df_standardized, "use_for_geocoding", BooleanType())
df_standardized = add_missing_column(df_standardized, "curation_note", StringType())

df_standardized = df_standardized.withColumn(
    "bairro_padronizado_key",
    lower(trim(col("bairro_padronizado")))
)

if GEOCODING_TABLE is not None:
    df_geo_raw = spark.table(GEOCODING_TABLE)
else:
    df_geo_raw = (
        df_standardized
        .select("id_bairro_canonico")
        .where(col("id_bairro_canonico").isNotNull())
        .dropDuplicates()
    )

df_geo_raw = add_missing_column(df_geo_raw, "id_bairro_canonico", IntegerType())
df_geo_raw = add_missing_column(df_geo_raw, "latitude", DoubleType())
df_geo_raw = add_missing_column(df_geo_raw, "longitude", DoubleType())
df_geo_raw = add_missing_column(df_geo_raw, "display_name", StringType())
df_geo_raw = add_missing_column(df_geo_raw, "osm_class", StringType())
df_geo_raw = add_missing_column(df_geo_raw, "osm_type", StringType())
df_geo_raw = add_missing_column(df_geo_raw, "osm_id", StringType())
df_geo_raw = add_missing_column(df_geo_raw, "candidate_score", IntegerType())
df_geo_raw = add_missing_column(df_geo_raw, "is_inside_aracatuba_bbox", BooleanType())
df_geo_raw = add_missing_column(df_geo_raw, "geocode_status", StringType())
df_geo_raw = add_missing_column(df_geo_raw, "geocode_source_final", StringType())

df_geo = (
    df_geo_raw
    .select(
        "id_bairro_canonico",
        col("latitude").alias("osm_latitude"),
        col("longitude").alias("osm_longitude"),
        col("display_name").alias("osm_display_name"),
        "osm_class",
        "osm_type",
        "osm_id",
        col("candidate_score").alias("osm_candidate_score"),
        col("is_inside_aracatuba_bbox").alias("osm_inside_bbox"),
        "geocode_status",
        "geocode_source_final",
    )
    .dropDuplicates(["id_bairro_canonico"])
)

if spark.catalog.tableExists(MANUAL_GEOCODING_TABLE):
    df_manual_geo = spark.table(MANUAL_GEOCODING_TABLE)
else:
    df_manual_geo = spark.createDataFrame([], schema=schema_manual_geo_empty)

df_manual_geo = add_missing_column(df_manual_geo, "bairro_padronizado", StringType())
df_manual_geo = add_missing_column(df_manual_geo, "manual_latitude", DoubleType())
df_manual_geo = add_missing_column(df_manual_geo, "manual_longitude", DoubleType())
df_manual_geo = add_missing_column(df_manual_geo, "manual_geocode_source", StringType())
df_manual_geo = add_missing_column(df_manual_geo, "manual_geocode_note", StringType())
df_manual_geo = add_missing_column(df_manual_geo, "manual_confidence", StringType())

if "bairro_padronizado_key" not in df_manual_geo.columns:
    df_manual_geo = df_manual_geo.withColumn(
        "bairro_padronizado_key",
        lower(trim(col("bairro_padronizado")))
    )

print(f"dim_bairro rows: {df_dim_bairro.count()}")
print(f"standardized rows: {df_standardized.count()}")
print(f"OSM geocoding rows: {df_geo.count()}")
print(f"manual geocoding rows: {df_manual_geo.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create geographic neighborhood dimension

# COMMAND ----------

# =========================
# 3. Create geographic neighborhood dimension
# =========================

# Keep only the columns needed from the manual geocoding table.
# This avoids ambiguous references such as bairro_padronizado from both s and m.
df_manual_geo_for_join = (
    df_manual_geo
    .select(
        "bairro_padronizado_key",
        "manual_latitude",
        "manual_longitude",
        "manual_geocode_source",
        "manual_geocode_note",
        "manual_confidence"
    )
    .dropDuplicates(["bairro_padronizado_key"])
)

df_dim_bairro_geografica = (
    df_dim_bairro.alias("b")
    .join(
        df_standardized.alias("s"),
        on="id_bairro",
        how="left"
    )
    .join(
        df_geo.alias("g"),
        on="id_bairro_canonico",
        how="left"
    )
    .join(
        df_manual_geo_for_join.alias("m"),
        on="bairro_padronizado_key",
        how="left"
    )
    .withColumn(
        "latitude",
        coalesce(col("m.manual_latitude"), col("g.osm_latitude"))
    )
    .withColumn(
        "longitude",
        coalesce(col("m.manual_longitude"), col("g.osm_longitude"))
    )
    .withColumn(
        "spatial_status",
        when(col("s.id_bairro_canonico").isNull(), lit("unresolved_name"))
        .when(col("s.use_for_geocoding") == False, lit("not_geocodable_source_name"))
        .when(
            col("m.manual_latitude").isNotNull() & col("m.manual_longitude").isNotNull(),
            lit("manual_reference_point")
        )
        .when(col("latitude").isNull() | col("longitude").isNull(), lit("missing_coordinates"))
        .when(col("g.osm_inside_bbox") == False, lit("outside_bbox"))
        .when(col("g.osm_candidate_score") < 9, lit("low_confidence_geocoding"))
        .when(lit(GEOCODING_SOURCE_MODE) == "validated", lit("validated_geocoding"))
        .when(lit(GEOCODING_SOURCE_MODE) == "draft", lit("draft_geocoding"))
        .otherwise(lit("geocoded"))
    )
    .withColumn(
        "spatial_quality",
        when(col("spatial_status") == "manual_reference_point", lit("medium"))
        .when(col("spatial_status") == "validated_geocoding", lit("high"))
        .when(col("spatial_status") == "draft_geocoding", lit("medium"))
        .when(col("spatial_status") == "low_confidence_geocoding", lit("low"))
        .when(col("spatial_status") == "outside_bbox", lit("low"))
        .otherwise(lit("unresolved"))
    )
    .withColumn(
        "is_mappable",
        when(
            col("latitude").isNotNull()
            & col("longitude").isNotNull()
            & col("spatial_status").isin(
                "manual_reference_point",
                "validated_geocoding",
                "draft_geocoding"
            ),
            lit(True)
        ).otherwise(lit(False))
    )
    .withColumn(
        "latitude_plot",
        coalesce(col("latitude"), lit(ARACATUBA_FALLBACK_LAT))
    )
    .withColumn(
        "longitude_plot",
        coalesce(col("longitude"), lit(ARACATUBA_FALLBACK_LON))
    )
    .withColumn(
        "geocode_source",
        coalesce(
            col("m.manual_geocode_source"),
            col("g.geocode_source_final"),
            lit(GEOCODING_SOURCE_MODE)
        )
    )
    .withColumn(
        "manual_geocode_note",
        col("m.manual_geocode_note")
    )
    .withColumn(
        "manual_confidence",
        col("m.manual_confidence")
    )
    .select(
        "id_bairro",
        col("b.bairro").alias("bairro_original_dim"),
        col("s.id_bairro_canonico").alias("id_bairro_canonico"),
        col("s.bairro_padronizado").alias("bairro_padronizado"),
        col("s.query_geocoding").alias("query_geocoding"),
        col("s.match_method").alias("match_method"),
        col("s.needs_review").alias("needs_review"),
        col("s.use_for_geocoding").alias("use_for_geocoding"),
        "latitude",
        "longitude",
        "latitude_plot",
        "longitude_plot",
        "is_mappable",
        "spatial_status",
        "spatial_quality",
        "geocode_source",
        "manual_confidence",
        col("g.osm_display_name").alias("osm_display_name"),
        col("g.osm_class").alias("osm_class"),
        col("g.osm_type").alias("osm_type"),
        col("g.osm_id").alias("osm_id"),
        col("g.osm_candidate_score").alias("osm_candidate_score"),
        col("g.geocode_status").alias("geocode_status"),
        col("s.curation_note").alias("curation_note"),
        "manual_geocode_note"
    )
    .orderBy("id_bairro")
)

df_dim_bairro_geografica.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(OUTPUT_TABLE)

print(f"Table saved as: {OUTPUT_TABLE}")
display(df_dim_bairro_geografica)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Create spatial views with geolocation

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {VW_CASOS_ESPACIAIS_GEO} AS
SELECT
    v.*,
    g.id_bairro,
    g.id_bairro_canonico,
    g.bairro_padronizado,
    g.latitude,
    g.longitude,
    g.latitude_plot,
    g.longitude_plot,
    g.is_mappable,
    g.spatial_status,
    g.spatial_quality,
    g.geocode_source,
    g.manual_confidence,
    g.osm_display_name AS bairro_display_name,
    g.osm_candidate_score,
    g.manual_geocode_note
FROM {VW_CASOS_ESPACIAIS} v
LEFT JOIN {DIM_BAIRRO_TABLE} b
    ON v.bairro = b.bairro
LEFT JOIN {OUTPUT_TABLE} g
    ON b.id_bairro = g.id_bairro
""")

spark.sql(f"""
CREATE OR REPLACE VIEW {VW_MAPA_CASOS_BAIRRO} AS
SELECT
    fonte,
    doenca,
    ano,
    mes,
    ordem_mes,
    ano_mes,
    bairro AS bairro_original,
    id_bairro,
    id_bairro_canonico,
    bairro_padronizado,
    latitude,
    longitude,
    latitude_plot,
    longitude_plot,
    is_mappable,
    spatial_status,
    spatial_quality,
    geocode_source,
    COUNT(*) AS casos
FROM {VW_CASOS_ESPACIAIS_GEO}
GROUP BY
    fonte,
    doenca,
    ano,
    mes,
    ordem_mes,
    ano_mes,
    bairro,
    id_bairro,
    id_bairro_canonico,
    bairro_padronizado,
    latitude,
    longitude,
    latitude_plot,
    longitude_plot,
    is_mappable,
    spatial_status,
    spatial_quality,
    geocode_source
""")

spark.sql(f"""
CREATE OR REPLACE VIEW {VW_MAPA_CASOS_BAIRRO_MAPEAVEL} AS
SELECT *
FROM {VW_MAPA_CASOS_BAIRRO}
WHERE is_mappable = true
""")

spark.sql(f"""
CREATE OR REPLACE VIEW {VW_QUALIDADE_ESPACIAL} AS
SELECT
    spatial_status,
    spatial_quality,
    is_mappable,
    COUNT(*) AS registros
FROM {VW_CASOS_ESPACIAIS_GEO}
GROUP BY
    spatial_status,
    spatial_quality,
    is_mappable
ORDER BY
    spatial_status,
    spatial_quality,
    is_mappable
""")

print(f"View saved as: {VW_CASOS_ESPACIAIS_GEO}")
print(f"View saved as: {VW_MAPA_CASOS_BAIRRO}")
print(f"View saved as: {VW_MAPA_CASOS_BAIRRO_MAPEAVEL}")
print(f"View saved as: {VW_QUALIDADE_ESPACIAL}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Validation and API smoke test

# COMMAND ----------

print("Spatial quality summary:")
display(spark.table(VW_QUALIDADE_ESPACIAL))

print("Mappability by disease and source:")
display(
    spark.sql(f"""
    SELECT
        fonte,
        doenca,
        COUNT(*) AS total_cases,
        SUM(CASE WHEN is_mappable = true THEN 1 ELSE 0 END) AS mappable_cases,
        SUM(CASE WHEN is_mappable = false THEN 1 ELSE 0 END) AS non_mappable_cases,
        ROUND(
            100.0 * SUM(CASE WHEN is_mappable = true THEN 1 ELSE 0 END) / COUNT(*),
            2
        ) AS mappable_percentage
    FROM {VW_CASOS_ESPACIAIS_GEO}
    GROUP BY fonte, doenca
    ORDER BY fonte, doenca
    """)
)

print("Map aggregation preview:")
display(
    spark.table(VW_MAPA_CASOS_BAIRRO)
    .orderBy("fonte", "doenca", "ano", "ordem_mes", "bairro_padronizado")
)

print("ETL_dim_bairro_geografica completed successfully.")
