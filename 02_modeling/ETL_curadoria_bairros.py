# Databricks notebook source
# MAGIC %md
# MAGIC # ETL_curadoria_bairros
# MAGIC
# MAGIC This notebook applies a manual curation layer after `ETL_padroniza_bairros`.
# MAGIC
# MAGIC It preserves the existing automated standardization output, adds a curated version for records that were not safely resolved, and rebuilds the geocoding input table used by `ETL_geocode_bairros_osm`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup
# MAGIC
# MAGIC Define catalog, schema, input tables, output tables, and helper settings.

# COMMAND ----------

from pyspark.sql.functions import (
    col,
    lit,
    coalesce,
    when,
    count as spark_count
)

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    BooleanType
)

CATALOG = "workspace"
SCHEMA = "default"
TABLE_PREFIX = f"{CATALOG}.{SCHEMA}"

REF_STANDARDIZED_TABLE = f"{TABLE_PREFIX}.ref_bairro_padronizado"
REF_STANDARDIZED_CURATED_TABLE = f"{TABLE_PREFIX}.ref_bairro_padronizado_curado"
REF_GEOCODING_INPUT_TABLE = f"{TABLE_PREFIX}.ref_bairros_para_geocoding"

print("Configuration loaded.")
print(f"Input standardized table: {REF_STANDARDIZED_TABLE}")
print(f"Curated standardized table: {REF_STANDARDIZED_CURATED_TABLE}")
print(f"Geocoding input table: {REF_GEOCODING_INPUT_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load standardized neighborhood table
# MAGIC
# MAGIC This table must already exist. It is created by `ETL_padroniza_bairros`.

# COMMAND ----------

df_base = spark.table(REF_STANDARDIZED_TABLE)

print("Base standardized table loaded.")
print(f"Rows: {df_base.count()}")

display(df_base.orderBy("bairro_original"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Define manual curation rules
# MAGIC
# MAGIC The rules below use `bairro_original_key` as the join key because it is more stable than the numerical `id_bairro`.
# MAGIC
# MAGIC Edit this list if new unresolved names appear in future runs.

# COMMAND ----------

manual_standardization_rows = [
    # bairro_original_key, id_bairro_canonico, bairro_padronizado, query_geocoding, match_method, use_for_geocoding, curation_note

    ("agua limpa", 900001, "Água Limpa", "Água Limpa, Araçatuba, São Paulo, Brasil", "manual_new_canonical", True, "Manual curation for an unresolved source neighborhood."),
    ("alphaville", 900002, "Alphaville", "Alphaville, Araçatuba, São Paulo, Brasil", "manual_new_canonical", True, "Manual curation for an unresolved source neighborhood."),
    ("aracatuba g", 900003, "Araçatuba G", "Araçatuba G, Araçatuba, São Paulo, Brasil", "manual_new_canonical", True, "Manual curation for an unresolved source neighborhood. Coordinate should be reviewed."),
    ("chacara daniel", 900004, "Chácara Daniel", "Chácara Daniel, Araçatuba, São Paulo, Brasil", "manual_new_canonical", True, "Manual curation for an unresolved source neighborhood."),
    ("chico mendes", 900005, "Chico Mendes", "Chico Mendes, Araçatuba, São Paulo, Brasil", "manual_new_canonical", True, "Manual curation for an unresolved source neighborhood."),
    ("condominio vitoria", 900006, "Condomínio Vitória", "Condomínio Vitória, Araçatuba, São Paulo, Brasil", "manual_new_canonical", True, "Manual curation for an unresolved source neighborhood."),

    # Generic or incomplete values should not be sent to geocoding.
    ("conjunto habitacional nao especificado", 900098, "Conjunto Habitacional não especificado", None, "manual_unresolved_generic", False, "Generic source name. It is not safe to geocode as a real neighborhood."),
    ("conjunto habitacional", 900098, "Conjunto Habitacional não especificado", None, "manual_unresolved_generic", False, "Generic source name. It is not safe to geocode as a real neighborhood."),
    ("conj habitacional", 900098, "Conjunto Habitacional não especificado", None, "manual_unresolved_generic", False, "Generic source name. It is not safe to geocode as a real neighborhood."),
    ("conj hab", 900098, "Conjunto Habitacional não especificado", None, "manual_unresolved_generic", False, "Generic source name. It is not safe to geocode as a real neighborhood."),

    ("corrego azul", 900007, "Córrego Azul", "Córrego Azul, Araçatuba, São Paulo, Brasil", "manual_new_canonical", True, "Manual curation for an unresolved source neighborhood."),
    ("fazenda paqueta", 900008, "Fazenda Paquetá", "Fazenda Paquetá, Araçatuba, São Paulo, Brasil", "manual_new_canonical", True, "Manual curation for an unresolved source neighborhood."),
    ("luana", 900009, "Jardim Luana", "Jardim Luana, Araçatuba, São Paulo, Brasil", "manual_new_canonical", True, "Source value appears shortened in the original data."),
    ("jardim luana", 900009, "Jardim Luana", "Jardim Luana, Araçatuba, São Paulo, Brasil", "manual_new_canonical", True, "Manual curation for an unresolved source neighborhood."),
    ("paqueta", 900010, "Paquetá", "Paquetá, Araçatuba, São Paulo, Brasil", "manual_new_canonical", True, "Manual curation for an unresolved source neighborhood."),
    ("residencial aimore", 900011, "Residencial Aimoré", "Residencial Aimoré, Araçatuba, São Paulo, Brasil", "manual_new_canonical", True, "Manual curation for an unresolved source neighborhood."),
    ("paquere", 900012, "Residencial Paquerê", "Residencial Paquerê, Araçatuba, São Paulo, Brasil", "manual_new_canonical", True, "Short source value mapped to the residential locality."),
    ("residencial paquere", 900012, "Residencial Paquerê", "Residencial Paquerê, Araçatuba, São Paulo, Brasil", "manual_new_canonical", True, "Manual curation for an unresolved source neighborhood."),

    # Common spelling variants.
    ("sylvio jose venturoli", 900013, "Sylvio José Venturoli", "Sylvio José Venturoli, Araçatuba, São Paulo, Brasil", "manual_new_canonical", True, "Manual curation for spelling variation."),
    ("silvio jose venturoli", 900013, "Sylvio José Venturoli", "Sylvio José Venturoli, Araçatuba, São Paulo, Brasil", "manual_new_canonical", True, "Manual curation for spelling variation."),
    ("sylvio venturoli", 900013, "Sylvio José Venturoli", "Sylvio José Venturoli, Araçatuba, São Paulo, Brasil", "manual_new_canonical", True, "Manual curation for spelling variation."),
    ("silvio venturoli", 900013, "Sylvio José Venturoli", "Sylvio José Venturoli, Araçatuba, São Paulo, Brasil", "manual_new_canonical", True, "Manual curation for spelling variation."),

    ("traitu", 900014, "Traitu", "Traitu, Araçatuba, São Paulo, Brasil", "manual_new_canonical", True, "Manual curation for an unresolved source neighborhood."),
    ("vilela", 900015, "Vilela", "Vilela, Araçatuba, São Paulo, Brasil", "manual_new_canonical", True, "Manual curation for an unresolved source neighborhood."),

    # Incomplete values.
    ("vil", 900099, "Bairro incompleto não especificado", None, "manual_unresolved_generic", False, "Incomplete source value. It is not safe to geocode."),
    ("vila", 900099, "Bairro incompleto não especificado", None, "manual_unresolved_generic", False, "Incomplete source value. It is not safe to geocode.")
]

schema_manual = StructType([
    StructField("bairro_original_key", StringType(), False),
    StructField("manual_id_bairro_canonico", IntegerType(), True),
    StructField("manual_bairro_padronizado", StringType(), True),
    StructField("manual_query_geocoding", StringType(), True),
    StructField("manual_match_method", StringType(), True),
    StructField("manual_use_for_geocoding", BooleanType(), True),
    StructField("curation_note", StringType(), True),
])

df_manual = spark.createDataFrame(manual_standardization_rows, schema=schema_manual)

manual_rule_count = df_manual.count()
manual_unique_key_count = df_manual.select("bairro_original_key").distinct().count()

if manual_rule_count != manual_unique_key_count:
    raise ValueError("There are duplicated bairro_original_key values in manual curation rules.")

print("Manual curation rules created.")
print(f"Manual rules: {manual_rule_count}")

display(df_manual.orderBy("bairro_original_key"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Check which manual rules match the current data
# MAGIC
# MAGIC This block helps identify manual rules that are no longer needed or source names that still need attention.

# COMMAND ----------

df_base_keys = df_base.select("bairro_original_key").distinct()

print("Manual rules that matched current source values:")
display(
    df_manual
    .join(df_base_keys, on="bairro_original_key", how="inner")
    .orderBy("bairro_original_key")
)

print("Manual rules not found in current source values:")
display(
    df_manual
    .join(df_base_keys, on="bairro_original_key", how="left_anti")
    .orderBy("bairro_original_key")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Apply manual curation
# MAGIC
# MAGIC The curated table keeps all original columns and adds:
# MAGIC
# MAGIC - `use_for_geocoding`
# MAGIC - `curation_note`
# MAGIC
# MAGIC Manual rules override the automated standardization only where a rule exists.

# COMMAND ----------

df_curated = (
    df_base.alias("b")
    .join(df_manual.alias("m"), on="bairro_original_key", how="left")
    .select(
        col("b.id_bairro"),
        col("b.bairro_original"),
        col("b.bairro_original_key"),
        coalesce(col("m.manual_id_bairro_canonico"), col("b.id_bairro_canonico")).alias("id_bairro_canonico"),
        coalesce(col("m.manual_bairro_padronizado"), col("b.bairro_padronizado")).alias("bairro_padronizado"),
        coalesce(col("m.manual_query_geocoding"), col("b.query_geocoding")).alias("query_geocoding"),
        coalesce(col("m.manual_match_method"), col("b.match_method")).alias("match_method"),
        col("b.similarity"),
        col("b.second_best_match"),
        col("b.second_similarity"),
        col("b.similarity_gap"),
        when(col("m.manual_match_method") == "manual_unresolved_generic", lit(True))
        .when(col("m.manual_match_method").isNotNull(), lit(False))
        .otherwise(col("b.needs_review"))
        .alias("needs_review"),
        coalesce(col("m.manual_use_for_geocoding"), lit(True)).alias("use_for_geocoding"),
        col("m.curation_note")
    )
)

print("Curation applied.")

display(
    df_curated
    .groupBy("match_method", "needs_review", "use_for_geocoding")
    .count()
    .orderBy("match_method", "needs_review", "use_for_geocoding")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Save curated standardization table
# MAGIC
# MAGIC This table is the controlled reference for the final spatial model.

# COMMAND ----------

(
    df_curated.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(REF_STANDARDIZED_CURATED_TABLE)
)

print(f"Saved curated table: {REF_STANDARDIZED_CURATED_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Rebuild geocoding input table
# MAGIC
# MAGIC The geocoding notebook already reads `ref_bairros_para_geocoding`.
# MAGIC
# MAGIC Therefore, this notebook overwrites that table with the curated version.

# COMMAND ----------

df_geocoding_input = (
    df_curated
    .filter(col("id_bairro_canonico").isNotNull())
    .filter(col("use_for_geocoding") == True)
    .filter(col("query_geocoding").isNotNull())
    .select(
        "id_bairro_canonico",
        col("bairro_padronizado").alias("bairro_canonico"),
        "query_geocoding"
    )
    .dropDuplicates(["id_bairro_canonico"])
    .orderBy("bairro_canonico")
)

(
    df_geocoding_input.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(REF_GEOCODING_INPUT_TABLE)
)

print(f"Updated geocoding input table: {REF_GEOCODING_INPUT_TABLE}")
print(f"Rows sent to geocoding: {df_geocoding_input.count()}")

display(df_geocoding_input)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Final curation validation
# MAGIC
# MAGIC The records below are not necessarily errors. They are records intentionally marked as review-needed or not safe for geocoding.

# COMMAND ----------

print("Records still marked as needing review:")
display(
    df_curated
    .filter(col("needs_review") == True)
    .orderBy("bairro_original")
)

print("Records not sent to geocoding:")
display(
    df_curated
    .filter(col("use_for_geocoding") == False)
    .select(
        "id_bairro",
        "bairro_original",
        "bairro_original_key",
        "bairro_padronizado",
        "match_method",
        "curation_note"
    )
    .orderBy("bairro_original")
)

print("Null check:")
display(
    df_curated
    .select(
        spark_count(when(col("id_bairro").isNull(), True)).alias("null_id_bairro"),
        spark_count(when(col("bairro_original").isNull(), True)).alias("null_bairro_original"),
        spark_count(when(col("id_bairro_canonico").isNull(), True)).alias("null_id_bairro_canonico"),
        spark_count(when(col("bairro_padronizado").isNull(), True)).alias("null_bairro_padronizado")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Notebook completed

# COMMAND ----------

print("ETL_curadoria_bairros completed successfully.")
