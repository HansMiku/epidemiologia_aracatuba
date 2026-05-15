# Databricks notebook source
# MAGIC %md
# MAGIC # ETL_validacao_espacial
# MAGIC
# MAGIC This notebook validates the final spatial model after `ETL_dim_bairro_geografica`.
# MAGIC
# MAGIC It does not transform data. It checks whether the spatial views are ready for the backend and dashboard.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup
# MAGIC
# MAGIC Define table names and helper functions.

# COMMAND ----------

from pyspark.sql.functions import col

CATALOG = "workspace"
SCHEMA = "default"
TABLE_PREFIX = f"{CATALOG}.{SCHEMA}"

DIM_BAIRRO_GEOGRAFICA_TABLE = f"{TABLE_PREFIX}.dim_bairro_geografica"
VW_CASOS_ESPACIAIS_GEO = f"{TABLE_PREFIX}.vw_casos_espaciais_geo"
VW_MAPA_CASOS_BAIRRO = f"{TABLE_PREFIX}.vw_mapa_casos_bairro"
VW_MAPA_CASOS_BAIRRO_MAPEAVEL = f"{TABLE_PREFIX}.vw_mapa_casos_bairro_mapeavel"
VW_QUALIDADE_ESPACIAL = f"{TABLE_PREFIX}.vw_qualidade_espacial"

required_objects = [
    DIM_BAIRRO_GEOGRAFICA_TABLE,
    VW_CASOS_ESPACIAIS_GEO,
    VW_MAPA_CASOS_BAIRRO
]

def table_exists(table_name: str) -> bool:
    try:
        spark.table(table_name).limit(1).collect()
        return True
    except Exception:
        return False

def get_scalar(sql_query: str, column_name: str):
    return spark.sql(sql_query).collect()[0][column_name]

print("Configuration loaded.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Check required objects
# MAGIC
# MAGIC The validation can only run after the final geographic dimension and spatial views have been created.

# COMMAND ----------

missing_objects = [obj for obj in required_objects if not table_exists(obj)]

if missing_objects:
    raise ValueError(
        "Missing required spatial objects. Run ETL_dim_bairro_geografica first: "
        + ", ".join(missing_objects)
    )

print("Required spatial objects found:")
for obj in required_objects:
    print(f"- {obj}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. General row counts
# MAGIC
# MAGIC These counts provide a quick overview of the final spatial layer.

# COMMAND ----------

print("dim_bairro_geografica:")
spark.sql(f"""
SELECT COUNT(*) AS rows
FROM {DIM_BAIRRO_GEOGRAFICA_TABLE}
""").show()

print("vw_casos_espaciais_geo:")
spark.sql(f"""
SELECT COUNT(*) AS rows
FROM {VW_CASOS_ESPACIAIS_GEO}
""").show()

print("vw_mapa_casos_bairro:")
spark.sql(f"""
SELECT COUNT(*) AS rows
FROM {VW_MAPA_CASOS_BAIRRO}
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Fatal validation checks
# MAGIC
# MAGIC These checks should return zero.
# MAGIC
# MAGIC If any of them fail, the spatial model is not ready for API consumption.

# COMMAND ----------

rows_geo = get_scalar(f"""
SELECT COUNT(*) AS value
FROM {VW_CASOS_ESPACIAIS_GEO}
""", "value")

rows_without_neighborhood_id = get_scalar(f"""
SELECT COUNT(*) AS value
FROM {VW_CASOS_ESPACIAIS_GEO}
WHERE id_bairro IS NULL
""", "value")

rows_without_spatial_status = get_scalar(f"""
SELECT COUNT(*) AS value
FROM {VW_CASOS_ESPACIAIS_GEO}
WHERE spatial_status IS NULL
""", "value")

rows_without_plot_coordinates = get_scalar(f"""
SELECT COUNT(*) AS value
FROM {VW_CASOS_ESPACIAIS_GEO}
WHERE latitude_plot IS NULL OR longitude_plot IS NULL
""", "value")

fatal_errors = []

if rows_geo == 0:
    fatal_errors.append("vw_casos_espaciais_geo has zero rows.")

if rows_without_neighborhood_id != 0:
    fatal_errors.append(f"{rows_without_neighborhood_id} rows have null id_bairro.")

if rows_without_spatial_status != 0:
    fatal_errors.append(f"{rows_without_spatial_status} rows have null spatial_status.")

if rows_without_plot_coordinates != 0:
    fatal_errors.append(f"{rows_without_plot_coordinates} rows have null plot coordinates.")

print("Fatal validation summary:")
print(f"Rows in spatial case view: {rows_geo}")
print(f"Rows without neighborhood ID: {rows_without_neighborhood_id}")
print(f"Rows without spatial status: {rows_without_spatial_status}")
print(f"Rows without plot coordinates: {rows_without_plot_coordinates}")

if fatal_errors:
    raise ValueError("Fatal spatial validation failed: " + " | ".join(fatal_errors))

print("Fatal checks passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Spatial quality summary
# MAGIC
# MAGIC These results are not necessarily errors. They describe how much of the spatial data is fully mappable.

# COMMAND ----------

display(
    spark.sql(f"""
    SELECT
        spatial_status,
        spatial_quality,
        is_mappable,
        COUNT(*) AS rows
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
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Mappability by disease and source
# MAGIC
# MAGIC This block helps decide what can be displayed on the map and what should be reported only as non-mappable records.

# COMMAND ----------

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
    GROUP BY
        fonte,
        doenca
    ORDER BY
        fonte,
        doenca
    """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Non-mappable records by neighborhood
# MAGIC
# MAGIC Use this output to manually improve coordinates or identify source-name limitations.

# COMMAND ----------

display(
    spark.sql(f"""
    SELECT
        bairro AS bairro_original,
        bairro_padronizado,
        spatial_status,
        spatial_quality,
        COUNT(*) AS cases
    FROM {VW_CASOS_ESPACIAIS_GEO}
    WHERE is_mappable = false
    GROUP BY
        bairro,
        bairro_padronizado,
        spatial_status,
        spatial_quality
    ORDER BY
        cases DESC,
        bairro
    """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Low-confidence geographic dimension records
# MAGIC
# MAGIC These records have coordinates or standardization problems that should be reviewed manually.

# COMMAND ----------

display(
    spark.sql(f"""
    SELECT
        id_bairro,
        bairro_original_dim,
        id_bairro_canonico,
        bairro_padronizado,
        query_geocoding,
        latitude,
        longitude,
        is_mappable,
        spatial_status,
        spatial_quality,
        geocode_source,
        osm_display_name,
        osm_candidate_score,
        curation_note,
        manual_geocode_note
    FROM {DIM_BAIRRO_GEOGRAFICA_TABLE}
    WHERE
        is_mappable = false
        OR spatial_quality IN ('low', 'unresolved')
        OR spatial_status IN ('osm_low_confidence', 'missing_coordinates', 'unresolved_name', 'not_geocodable_source_name')
    ORDER BY
        spatial_quality,
        spatial_status,
        bairro_original_dim
    """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Map aggregation preview
# MAGIC
# MAGIC This is the main backend-ready aggregation for the spatial dashboard.

# COMMAND ----------

display(
    spark.table(VW_MAPA_CASOS_BAIRRO)
    .orderBy("fonte", "doenca", "ano", "ordem_mes", "bairro_padronizado")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Mappable map aggregation preview
# MAGIC
# MAGIC If the mappable-only view exists, it should be used by the main Leaflet map layer.

# COMMAND ----------

if table_exists(VW_MAPA_CASOS_BAIRRO_MAPEAVEL):
    display(
        spark.table(VW_MAPA_CASOS_BAIRRO_MAPEAVEL)
        .orderBy("fonte", "doenca", "ano", "ordem_mes", "bairro_padronizado")
    )
else:
    print(f"Optional view not found: {VW_MAPA_CASOS_BAIRRO_MAPEAVEL}")
    print("This is not fatal if the dashboard filters vw_mapa_casos_bairro with is_mappable = true.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Spatial quality view preview
# MAGIC
# MAGIC This optional view is useful for dashboard cards and the project report.

# COMMAND ----------

if table_exists(VW_QUALIDADE_ESPACIAL):
    display(
        spark.table(VW_QUALIDADE_ESPACIAL)
        .orderBy("spatial_status", "spatial_quality", "is_mappable")
    )
else:
    print(f"Optional view not found: {VW_QUALIDADE_ESPACIAL}")
    print("This is not fatal, but it is useful to create it in ETL_dim_bairro_geografica.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Final validation result

# COMMAND ----------

print("ETL_validacao_espacial completed successfully.")
print("The final spatial model passed fatal checks.")
print("Review non-mappable or low-confidence records only if you want to increase map coverage.")
