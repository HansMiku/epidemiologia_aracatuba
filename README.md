# Epidemiological Data Pipeline - Araçatuba/SP

This repository contains Databricks notebooks for transforming public epidemiological datasets from Araçatuba, São Paulo, Brazil, into structured Delta tables and SQL views suitable for analytical dashboards and web application consumption.

The project covers historical and spatial epidemiological data related to COVID-19, arboviruses, scorpion accidents, and case records by neighborhood. The pipeline includes data ingestion, cleaning, standardization, dimensional modeling, neighborhood name standardization, geographic enrichment, and validation.

Database object names and field names are kept in Portuguese, such as `dim_tempo`, `fato_*`, `doenca`, and `bairro`, to preserve compatibility with the implemented ETL notebooks, the database model, and the technical report. Repository documentation is written in English.

## Project objective

The objective is to organize heterogeneous public health spreadsheets into a consistent analytical data model that can support:

- temporal analysis of epidemiological cases;
- spatial visualization by neighborhood when reliable location data are available;
- disease and period filters for dashboards;
- data quality and spatial coverage indicators;
- future integration with a FastAPI backend and a Bootstrap/Leaflet frontend.

## Target architecture

```text
Public epidemiological CSV files
    ↓
Raw tables in Databricks
    ↓
Source ETL notebooks
    ↓
Intermediate Delta fact tables
    ↓
Dimension and final fact modeling
    ↓
Neighborhood standardization and curation
    ↓
Geocoding and geographic curation
    ↓
Spatial validation
    ↓
SQL views for backend and dashboard consumption
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

### 1. Source ETL notebooks

These notebooks transform raw public datasets into intermediate Delta fact tables.

1. `ETL_covid_serie_historica`
2. `ETL_arboviroses`
3. `ETL_escorpiao_serie_historica`
4. `ETL_dengue_espacial`
5. `ETL_chikungunya_espacial`

### 2. Dimensional modeling

6. `ETL_dimensoes`

This notebook creates the base dimensions, final fact tables, and first SQL views for analytical consumption.

### 3. Neighborhood standardization and spatial enrichment

7. `ETL_padroniza_bairros`
8. `ETL_curadoria_bairros`
9. `ETL_geocode_bairros_osm`
10. `ETL_curadoria_geografica_manual`
11. `ETL_dim_bairro_geografica`
12. `ETL_validacao_espacial`

The spatial enrichment stage is separated from the epidemiological ETL because neighborhood names and geographic coordinates require additional curation. This design avoids silently converting uncertain names into misleading map points.

## Intermediate tables

The source ETL notebooks create the following intermediate Delta tables:

- `workspace.default.fato_covid`
- `workspace.default.fato_arboviroses`
- `workspace.default.fato_escorpiao`
- `workspace.default.fato_dengue_espacial`
- `workspace.default.fato_chikungunya_espacial`

These tables preserve source-specific structures before the final analytical model is created.

## Final analytical model

`ETL_dimensoes` creates the main analytical model.

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

## Coordinate priority rule

`ETL_dim_bairro_geografica` applies the following priority rule for coordinates:

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

This view contains only records classified as safely mappable.

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

## Validation strategy

The project includes two validation levels.

### Base analytical validation

`ETL_dimensoes` validates the base analytical model by checking:

- final table and view row counts;
- null surrogate keys in final fact tables;
- original versus final row counts;
- removal of `exame` from the final dengue spatial table and spatial view;
- smoke tests for `vw_serie_temporal` and `vw_casos_espaciais`.

### Spatial validation

`ETL_validacao_espacial` validates the final spatial model by checking:

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

## Current limitations

- The geographic layer uses point references, not official neighborhood polygons.
- Manually curated coordinates are reference points and should not be interpreted as official centroids.
- OSM/Nominatim results require review before being treated as validated coordinates.
- Some neighborhood names in public bulletins are incomplete, generic, inconsistent, or not safely geocodable.
- Chikungunya spatial data does not contain daily dates and therefore uses `ano_referencia`.
- Public source files may contain formatting inconsistencies because they come from heterogeneous bulletins.

## Repository and security notes

Credentials, tokens, connection strings, local environment files, and personal access tokens are not part of this repository.

Databricks HTML exports with UUID filenames should be renamed with descriptive notebook names before publication. When available, `.ipynb` exports are preferable for code review and reproducibility.


