# Neighborhood Standardization and Geocoding Pipeline

This document describes the spatial enrichment stage of the Araçatuba/SP epidemiological data pipeline.

The purpose of this stage is to standardize neighborhood names, create controlled geocoding inputs, obtain or curate coordinates, build a geographic neighborhood dimension, validate map-ready SQL views, and prepare the spatial model for Supabase PostgreSQL export and dashboard use.

## Repository placement

The spatial enrichment notebooks are stored in the `02_modeling/` folder:

```text
epidemiologia_aracatuba/
└── 02_modeling/
    ├── ETL_padroniza_bairros
    ├── ETL_curadoria_bairros
    ├── ETL_geocode_bairros_osm
    ├── ETL_curadoria_geografica_manual
    ├── ETL_dim_bairro_geografica
    └── ETL_validacao_espacial
```

The base dimensional notebook is also in `02_modeling/` and must be executed before the spatial enrichment stage:

```text
02_modeling/ETL_dimensoes
```

## Rationale

Neighborhood names in public epidemiological bulletins are not always standardized. Records may contain abbreviations, spelling variations, generic labels, incomplete names, or names that cannot be safely geocoded automatically.

The pipeline therefore separates epidemiological transformation from geographic enrichment. It preserves original neighborhood values, creates normalized comparison keys, applies curated standardization rules, and classifies records according to spatial quality instead of forcing every record into map coordinates.

## Required previous steps

The spatial enrichment notebooks depend on the outputs of the raw ETL and dimensional modeling stages.

The following notebooks must be executed first:

1. `01_raw_etl/ETL_covid_serie_historica`
2. `01_raw_etl/ETL_arboviroses`
3. `01_raw_etl/ETL_escorpiao_serie_historica`
4. `01_raw_etl/ETL_dengue_espacial`
5. `01_raw_etl/ETL_chikungunya_espacial`
6. `02_modeling/ETL_dimensoes`

At minimum, the following objects must exist before the spatial stage starts:

- `workspace.default.dim_bairro`
- `workspace.default.vw_casos_espaciais`

## Required reference tables

`02_modeling/ETL_padroniza_bairros` reads two reference tables from Databricks:

- `workspace.default.canonical_neighborhoods`
- `workspace.default.alias_seed_rules`

These tables can be created from CSV files before running the notebook.

### Expected columns in `canonical_neighborhoods`

- `id_bairro_canonico`
- `bairro_canonico`
- `bairro_canonico_norm`
- `municipio`
- `uf`
- `pais`
- `tipo_localidade`
- `use_for_geocoding`
- `query_geocoding`
- `source_primary_url`
- `source_secondary_url`
- `observacao`

### Expected columns in `alias_seed_rules`

- `alias_norm_observado`
- `bairro_canonico_sugerido`
- `observacao`

## Execution order

The spatial notebooks should be executed in this order:

1. `02_modeling/ETL_padroniza_bairros`
2. `02_modeling/ETL_curadoria_bairros`
3. `02_modeling/ETL_geocode_bairros_osm`
4. `02_modeling/ETL_curadoria_geografica_manual`
5. `02_modeling/ETL_dim_bairro_geografica`
6. `02_modeling/ETL_validacao_espacial`

After spatial validation, the Supabase export can be executed through:

```text
03_export/EXPORT_supabase_postgresql
```

## Notebook responsibilities

### `ETL_padroniza_bairros`

Creates normalized comparison keys and applies canonical and alias matching rules.

Main outputs:

- `workspace.default.ref_bairros_canonicos`
- `workspace.default.ref_bairros_alias`
- `workspace.default.ref_bairro_padronizado`
- `workspace.default.ref_bairros_para_geocoding`

The notebook preserves the original `dim_bairro` values and does not overwrite the base neighborhood dimension.

### `ETL_curadoria_bairros`

Applies manual standardization rules for records that are not safely resolved by automated matching.

Main outputs:

- `workspace.default.ref_bairro_padronizado_curado`
- `workspace.default.ref_bairros_para_geocoding`

Generic or incomplete source labels can be marked as not suitable for geocoding through `use_for_geocoding = false`.

### `ETL_geocode_bairros_osm`

Uses Nominatim/OpenStreetMap to create a draft coordinate table from `ref_bairros_para_geocoding`.

Main outputs:

- `workspace.default.bairro_geocoding_draft`
- `workspace.default.bairro_geocoding_validated`, created only when `APPROVE_AUTOMATIC_GEOCODING = True`

The notebook includes:

- custom user-agent configuration;
- delay between requests;
- approximate Araçatuba bounding-box checks;
- candidate scoring based on location, place type, and display name;
- manual approval logic before creating the validated table.

The draft table must exist before subsequent review or validation steps are executed.

### `ETL_curadoria_geografica_manual`

Creates a manually curated geographic reference table for neighborhoods that require reliable reference points beyond automatic OSM/Nominatim results.

Main outputs:

- `workspace.default.ref_bairro_geocoding_manual`
- `workspace.default.ref_bairro_geocoding_manual_review_queue`, optional and created when `vw_casos_espaciais_geo` already exists

Manual coordinates are treated as reference points, not official neighborhood centroids or official polygons.

### `ETL_dim_bairro_geografica`

Creates the final geographic neighborhood dimension and spatial consumption views.

Coordinate priority rule:

1. manual reference points from `ref_bairro_geocoding_manual`;
2. validated or draft OSM/Nominatim coordinates;
3. municipal fallback coordinates only for plotting support and classification.

Main outputs:

- `workspace.default.dim_bairro_geografica`
- `workspace.default.vw_casos_espaciais_geo`
- `workspace.default.vw_mapa_casos_bairro`
- `workspace.default.vw_mapa_casos_bairro_mapeavel`
- `workspace.default.vw_qualidade_espacial`

The final model includes:

- `latitude`
- `longitude`
- `latitude_plot`
- `longitude_plot`
- `is_mappable`
- `spatial_status`
- `spatial_quality`
- `geocode_source`

### `ETL_validacao_espacial`

Validates the final spatial model. It does not transform data.

The notebook checks:

- required spatial objects;
- row counts;
- null `id_bairro` values;
- null `spatial_status` values;
- null plotting coordinates;
- mappability by disease and source;
- non-mappable records by neighborhood;
- low-confidence or unresolved geographic records;
- map aggregation previews.

Fatal validation errors must be corrected before the spatial views are exposed through the backend.

## Spatial views

The spatial model provides four main views for dashboard and backend integration.

| View | Purpose |
|---|---|
| `vw_casos_espaciais_geo` | Detailed spatial case-level view with geographic metadata. |
| `vw_mapa_casos_bairro` | Aggregated cases by disease, source, time, and neighborhood. |
| `vw_mapa_casos_bairro_mapeavel` | Map-ready subset where `is_mappable = true`. |
| `vw_qualidade_espacial` | Spatial quality summary for cards, indicators, and report discussion. |

The recommended view for the main Leaflet map layer is:

```sql
workspace.default.vw_mapa_casos_bairro_mapeavel
```

Non-mappable records should be reported separately through `vw_qualidade_espacial` or filtered records from `vw_casos_espaciais_geo`.

## Interpretation rules

- `latitude` and `longitude` represent neighborhood-level coordinates only when `is_mappable = true`.
- `latitude_plot` and `longitude_plot` may contain fallback coordinates and must not be interpreted as real neighborhood coordinates.
- `manual_reference_point` indicates a manually curated reference point.
- `validated_geocoding` indicates an approved OSM/Nominatim result.
- `draft_geocoding` indicates an available coordinate that still requires caution.
- `missing_coordinates`, `unresolved_name`, and `not_geocodable_source_name` represent data quality limitations and should not be plotted as precise locations.
- Non-mappable records are part of the data quality analysis and should not be removed from the project documentation.

## Supabase export relationship

The spatial model is exported to Supabase through:

```text
03_export/EXPORT_supabase_postgresql
```

The export includes the final geographic dimension:

- `public.dim_bairro_geografica`

When the export notebook is configured with `CREATE_CONSUMPTION_VIEWS = True`, it creates or replaces the following spatial views in Supabase:

- `public.vw_casos_espaciais_geo`
- `public.vw_mapa_casos_bairro`
- `public.vw_mapa_casos_bairro_mapeavel`
- `public.vw_qualidade_espacial`

The recommended Supabase view for the frontend map layer is:

```sql
public.vw_mapa_casos_bairro_mapeavel
```

The recommended Supabase view for spatial quality indicators is:

```sql
public.vw_qualidade_espacial
```

## Quality and reproducibility considerations

- Automatic geocoding results must be reviewed before validation.
- Manual coordinates are approximate reference points and should be documented as such.
- Public bulletins may contain inconsistent or incomplete neighborhood names.
- The absence of a mappable coordinate is preserved as information rather than hidden by forced geocoding.
- The spatial validation notebook should be part of every full pipeline run before dashboard publication.
- Supabase export should be executed only after spatial validation is successful.

## Repository and security notes

Credentials, tokens, local configuration files, Databricks secrets, and Supabase connection strings with passwords are not part of this repository.

Databricks HTML exports with UUID filenames should be renamed using descriptive notebook names. `.ipynb` exports are preferable when available.
