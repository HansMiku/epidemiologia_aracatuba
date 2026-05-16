# Epidemiological Data Pipeline - Araçatuba/SP

This repository contains the Databricks pipeline used to transform public epidemiological datasets from Araçatuba, São Paulo, Brazil, into analytical Delta tables, spatial views, and Supabase PostgreSQL tables for dashboard and web application consumption.

The project covers temporal and spatial epidemiological data related to COVID-19, arboviruses, scorpion accidents, dengue cases by neighborhood, and chikungunya cases by neighborhood. The pipeline includes raw data ingestion, cleaning, standardization, dimensional modeling, neighborhood name curation, geographic enrichment, spatial validation, and export to Supabase PostgreSQL.

Database object names and field names are kept in Portuguese, such as `dim_tempo`, `fato_*`, `doenca`, and `bairro`, to preserve compatibility with the implemented ETL notebooks, the relational model, and the technical report. Repository documentation is written in English.

## Project objective

The objective is to organize heterogeneous public health spreadsheets into a consistent analytical data model that can support:

- temporal analysis of epidemiological cases;
- spatial visualization by neighborhood when reliable location data are available;
- disease, source, and period filters for dashboards;
- spatial data quality indicators;
- integration with a FastAPI backend and a Bootstrap/Leaflet frontend.

## Current repository structure

The Databricks workspace is organized by pipeline stage:

```text
epidemiologia_aracatuba/
├── 01_raw_etl/
│   ├── ETL_arboviroses
│   ├── ETL_chikungunya_espacial
│   ├── ETL_covid_serie_historica
│   ├── ETL_dengue_espacial
│   └── ETL_escorpiao_serie_historica
├── 02_modeling/
│   ├── ETL_curadoria_bairros
│   ├── ETL_curadoria_geografica_manual
│   ├── ETL_dim_bairro_geografica
│   ├── ETL_dimensoes
│   ├── ETL_geocode_bairros_osm
│   ├── ETL_padroniza_bairros
│   └── ETL_validacao_espacial
├── 03_export/
│   └── EXPORT_supabase_postgresql
├── README.md
└── README_geocoding.md
```

If the notebooks are exported from Databricks before publication, the same logical structure should be preserved. Depending on the export format, notebook filenames may receive extensions such as `.ipynb`, `.py`, or `.html`.

## Target architecture

```text
Public epidemiological CSV files
    ↓
Raw tables in Databricks
    ↓
01_raw_etl notebooks
    ↓
Intermediate Delta fact tables
    ↓
02_modeling notebooks
    ↓
Final dimensions, final facts, and analytical views
    ↓
Neighborhood standardization and geographic enrichment
    ↓
Spatial validation
    ↓
03_export notebook
    ↓
Supabase PostgreSQL
    ↓
FastAPI backend
    ↓
Bootstrap and Leaflet frontend dashboard
```

## Databricks environment

The notebooks were designed for Databricks using Delta tables in the following namespace:

```text
workspace.default
```

The expected raw input tables are:

- `workspace.default.raw_covid`
- `workspace.default.raw_arboviroses`
- `workspace.default.raw_escorpiao`
- `workspace.default.raw_dengue_espacial`
- `workspace.default.raw_chikungunya_espacial`

The neighborhood standardization stage also requires reference tables:

- `workspace.default.canonical_neighborhoods`
- `workspace.default.alias_seed_rules`

These reference tables may be created from CSV files before running the neighborhood standardization notebooks.

## Recommended execution order

### 1. Raw ETL stage

Run the notebooks in `01_raw_etl/` first. These notebooks transform raw public datasets into intermediate Delta fact tables.

1. `01_raw_etl/ETL_covid_serie_historica`
2. `01_raw_etl/ETL_arboviroses`
3. `01_raw_etl/ETL_escorpiao_serie_historica`
4. `01_raw_etl/ETL_dengue_espacial`
5. `01_raw_etl/ETL_chikungunya_espacial`

### 2. Modeling and spatial enrichment stage

Run the notebooks in `02_modeling/` after the raw ETL stage.

6. `02_modeling/ETL_dimensoes`
7. `02_modeling/ETL_padroniza_bairros`
8. `02_modeling/ETL_curadoria_bairros`
9. `02_modeling/ETL_geocode_bairros_osm`
10. `02_modeling/ETL_curadoria_geografica_manual`
11. `02_modeling/ETL_dim_bairro_geografica`
12. `02_modeling/ETL_validacao_espacial`

The spatial enrichment stage is separated from the source ETL because neighborhood names and geographic coordinates require additional curation. This design avoids silently converting uncertain names into misleading map points.

### 3. Supabase export stage

Run the notebook in `03_export/` only after the Databricks analytical and spatial validation steps are complete.

13. `03_export/EXPORT_supabase_postgresql`

This notebook exports the final Databricks tables to Supabase PostgreSQL and can recreate PostgreSQL consumption views for the backend and dashboard.

## Intermediate tables

The raw ETL notebooks create the following intermediate Delta tables:

- `workspace.default.fato_covid`
- `workspace.default.fato_arboviroses`
- `workspace.default.fato_escorpiao`
- `workspace.default.fato_dengue_espacial`
- `workspace.default.fato_chikungunya_espacial`

These tables preserve source-specific structures before the final analytical model is created.

## Final analytical model

`02_modeling/ETL_dimensoes` creates the main analytical model.

### Dimension tables

- `workspace.default.dim_tempo`
- `workspace.default.dim_doenca`
- `workspace.default.dim_bairro`
- `workspace.default.dim_sexo`

### Final fact tables

- `workspace.default.fato_arboviroses_final`
- `workspace.default.fato_covid_final`
- `workspace.default.fato_escorpiao_final`
- `workspace.default.fato_dengue_espacial_final`
- `workspace.default.fato_chikungunya_espacial_final`

### Base analytical views

- `workspace.default.vw_serie_temporal`
- `workspace.default.vw_casos_espaciais`

## Spatial enrichment model

The spatial pipeline creates reference, curation, geocoding, and final geographic objects.

### Neighborhood reference and standardization tables

- `workspace.default.ref_bairros_canonicos`
- `workspace.default.ref_bairros_alias`
- `workspace.default.ref_bairro_padronizado`
- `workspace.default.ref_bairro_padronizado_curado`
- `workspace.default.ref_bairros_para_geocoding`

### Geocoding and manual curation tables

- `workspace.default.bairro_geocoding_draft`
- `workspace.default.bairro_geocoding_validated`
- `workspace.default.ref_bairro_geocoding_manual`
- `workspace.default.ref_bairro_geocoding_manual_review_queue`

### Final geographic table and spatial views

- `workspace.default.dim_bairro_geografica`
- `workspace.default.vw_casos_espaciais_geo`
- `workspace.default.vw_mapa_casos_bairro`
- `workspace.default.vw_mapa_casos_bairro_mapeavel`
- `workspace.default.vw_qualidade_espacial`

The recommended Databricks view for the main Leaflet map layer is:

```sql
workspace.default.vw_mapa_casos_bairro_mapeavel
```

## Coordinate priority rule

`02_modeling/ETL_dim_bairro_geografica` applies the following priority rule for coordinates:

1. manually curated reference points from `ref_bairro_geocoding_manual`;
2. validated or draft OSM/Nominatim results from `bairro_geocoding_validated` or `bairro_geocoding_draft`;
3. municipal fallback coordinates only for plotting support and quality classification.

Fallback coordinates are not treated as real neighborhood coordinates. They are stored only in plotting fields so that non-mappable records can be classified, counted, and reported without generating false neighborhood-level map points.

## Spatial quality fields

The final geographic model includes quality-control fields designed for backend and dashboard use:

- `is_mappable`: indicates whether a record can be safely plotted on the map;
- `spatial_status`: describes why a record is mappable or non-mappable;
- `spatial_quality`: classifies coordinate quality as `high`, `medium`, `low`, or `unresolved`;
- `geocode_source`: identifies whether the coordinate came from manual curation, OSM/Nominatim, or fallback logic;
- `latitude_plot` and `longitude_plot`: plotting coordinates, which may include fallback values for non-mappable records.

The recommended view for the main Leaflet map layer is:

```sql
workspace.default.vw_mapa_casos_bairro_mapeavel
```

Only records classified as safely mappable should be rendered as neighborhood points in the dashboard map.

## Supabase PostgreSQL export

`03_export/EXPORT_supabase_postgresql` exports the final Databricks Delta tables from `workspace.default` to Supabase PostgreSQL.

The export notebook uses Python `psycopg` to write to PostgreSQL. This avoids Spark JDBC write limitations in Databricks Free/Serverless environments. The notebook still reads the source tables from Databricks with Spark.

### Required Databricks Secrets

Supabase credentials must be stored in Databricks Secrets and must not be hard-coded in the notebook or committed to GitHub.

Expected secret configuration:

```text
Scope: supabase
Key: jdbc_url
Key: user
Key: password
```

Expected `jdbc_url` format for the Supabase Session Pooler:

```text
jdbc:postgresql://aws-1-sa-east-1.pooler.supabase.com:5432/postgres?sslmode=require
```

Expected `user` format for the Supabase Session Pooler:

```text
postgres.<project-ref>
```

### Tables exported to Supabase

The export order is important because fact tables depend on dimension tables through foreign keys.

1. `dim_tempo`
2. `dim_doenca`
3. `dim_bairro`
4. `dim_sexo`
5. `dim_bairro_geografica`
6. `fato_arboviroses_final`
7. `fato_covid_final`
8. `fato_escorpiao_final`
9. `fato_dengue_espacial_final`
10. `fato_chikungunya_espacial_final`

The target PostgreSQL tables must already exist in Supabase with compatible columns. The notebook aligns the Databricks DataFrame to the existing target table columns before inserting rows.

### Supabase consumption views

When `CREATE_CONSUMPTION_VIEWS = True`, the export notebook creates or replaces the following PostgreSQL views:

- `public.vw_serie_temporal`
- `public.vw_casos_espaciais`
- `public.vw_casos_espaciais_geo`
- `public.vw_mapa_casos_bairro`
- `public.vw_mapa_casos_bairro_mapeavel`
- `public.vw_qualidade_espacial`

After exporting, the following SQL checks can be run in Supabase:

```sql
select * from public.vw_serie_temporal limit 10;
select * from public.vw_mapa_casos_bairro_mapeavel limit 10;
select * from public.vw_qualidade_espacial;
```

## Main transformations and modeling decisions

- Standardized reads and writes using `workspace.default`.
- Converted wide historical tables into long analytical formats.
- Preserved disease-specific intermediate tables before creating final fact tables.
- Standardized month names and created month-order fields for time-series analysis.
- Removed non-numeric characters and thousands separators before numeric casting.
- Removed unpublished months when case values are null instead of treating them as zero.
- Added disease identifiers to final fact tables.
- Converted dengue spatial dates to date type.
- Normalized neighborhood and sex fields by removing accents, trimming spaces, and standardizing case.
- Removed the `exame` field from the final dengue spatial model because it is not used in the temporal or spatial dashboard model.
- Created final dimensions and surrogate-key-based fact tables.
- Created SQL views for temporal charts, spatial case analysis, map layers, and spatial quality monitoring.
- Added neighborhood standardization, alias matching, manual curation, OSM/Nominatim geocoding, manual coordinate curation, and spatial validation layers.
- Exported the final analytical model to Supabase PostgreSQL for external application consumption.

## Validation strategy

The project includes three validation levels.

### Base analytical validation

`02_modeling/ETL_dimensoes` validates the base analytical model by checking:

- final table and view row counts;
- null surrogate keys in final fact tables;
- original versus final row counts;
- removal of `exame` from the final dengue spatial table and spatial view;
- smoke tests for `vw_serie_temporal` and `vw_casos_espaciais`.

### Spatial validation

`02_modeling/ETL_validacao_espacial` validates the final spatial model by checking:

- existence of required spatial objects;
- row counts in spatial views;
- null `id_bairro` values;
- null `spatial_status` values;
- null plotting coordinates;
- mappability by disease and source;
- non-mappable records by neighborhood;
- low-confidence or unresolved geographic dimension records;
- map aggregation previews.

Fatal validation errors must be corrected before exposing the spatial views through the backend.

### Supabase export validation

`03_export/EXPORT_supabase_postgresql` validates the PostgreSQL export by checking row counts for exported tables and selected consumption views after loading.

## Current limitations

- The geographic layer uses point references, not official neighborhood polygons.
- Manually curated coordinates are reference points and should not be interpreted as official centroids.
- OSM/Nominatim results require review before being treated as validated coordinates.
- Some neighborhood names in public bulletins are incomplete, generic, inconsistent, or not safely geocodable.
- Chikungunya spatial data does not contain daily dates and therefore uses `ano_referencia`.
- Public source files may contain formatting inconsistencies because they come from heterogeneous bulletins.
- Supabase export depends on the existence of target PostgreSQL tables with compatible schemas.

## Repository and security notes

Credentials, tokens, connection strings with passwords, local environment files, and personal access tokens must not be committed to this repository.

Before committing exported notebooks to GitHub:

- clear notebook outputs when possible;
- confirm that no password or full credential-bearing connection string is present;
- keep Supabase credentials only in Databricks Secrets;
- avoid committing local temporary exports or sensitive data files;
- preserve the current folder structure by pipeline stage.

## Version control and ignored files

This repository includes a `.gitignore` file to prevent local configuration files, credentials, temporary files, notebook checkpoints, and local exports from being committed.

The `.gitignore` is especially important because the project uses Databricks Secrets and Supabase PostgreSQL credentials. Credentials, tokens, connection strings with passwords, local environment files, and personal access tokens must never be committed to GitHub.

The ignored files include:

- local environment files, such as `.env`;
- token, key, certificate, and Databricks archive files;
- Python cache files and notebook checkpoints;
- local editor and operating system files;
- optional local data exports;
- temporary and backup files.
