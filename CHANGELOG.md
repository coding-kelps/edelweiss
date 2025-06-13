# Changelog

All notable changes to this project will be documented in this file.

## [0.10.0] - 2025-06-13

### ⛰️  Features

- *(assets)* Add date to occurrences - ([f73587e](https://github.com/coding-kelps/edelweiss/commit/f73587eea7079286fa7628e802dbe673c87d21a7))
- *(assets)* Add species scientific name to operational database - ([8fad1a3](https://github.com/coding-kelps/edelweiss/commit/8fad1a3e246b374ad4c31147efcf206c7708b86d))
- *(assets)* Add two new assets species_names and occurrences to load more information to operational database - ([78e7133](https://github.com/coding-kelps/edelweiss/commit/78e71333fbfc673d1ca428b02a240f37502a0bba))
- *(assets)* Add new asset "species_occurrences" to load occurrences spatial multipoints to PostgreSQL - ([8eda448](https://github.com/coding-kelps/edelweiss/commit/8eda448c4d373de02a35d7042b2f1e5d9e9ca1e9))
- *(assets)* Add lat long values convertion to geospatial data structure with newly added geospatial_occurrences and occurrences_by_taxon_key tables - ([537edd9](https://github.com/coding-kelps/edelweiss/commit/537edd93b1fa6f86d7fc3ee44f743e1f530a7d9e))
- *(resources)* Download GBIF dataset in temporary directories - ([0f88aba](https://github.com/coding-kelps/edelweiss/commit/0f88aba6cdc5f328204624bd0bd2a1e9e679dde5))
- *(tegola)* Add taxon_key filter to display only one species in the map - ([f2547f3](https://github.com/coding-kelps/edelweiss/commit/f2547f3877affb3d4d33d3255ad3fc5b109c4a0e))
- Separate data ingestion from operational loading - ([f1ec11d](https://github.com/coding-kelps/edelweiss/commit/f1ec11d6f29b884b7ebddedf4716df21434816e7))
- Add vernaculars in both french and deutsch in addition to english - ([d4e2d71](https://github.com/coding-kelps/edelweiss/commit/d4e2d714e94f6d68b82f78bdafd11031f1b9267f))
- Add check for previous GBIF download with similar request - ([e0291a6](https://github.com/coding-kelps/edelweiss/commit/e0291a6451ec62d47946b41937c6e41a9ca3f412))
- Add sensor to request GBIF download - ([b402b48](https://github.com/coding-kelps/edelweiss/commit/b402b482523042a36ade46c7ce6e99f75c764b77))
- Use wrapper of GBIF API call when retrieving species vernacular name - ([f5b98cf](https://github.com/coding-kelps/edelweiss/commit/f5b98cf39af385e05febac77333410e560abba23))
- Download GBIF subset from generated_gbif_download_keys - ([2db4b0a](https://github.com/coding-kelps/edelweiss/commit/2db4b0acc71eeafce5792197545935ab9e6b8e22))
- Replace tables when already exists - ([9d5d9f3](https://github.com/coding-kelps/edelweiss/commit/9d5d9f366f5e56851be7fae0856df5dd2f9aeef2))
- Add GBIF download keys generation - ([0fe8d14](https://github.com/coding-kelps/edelweiss/commit/0fe8d14eb69cb7751bc35c9164768aff958a2ccd))
- Add configuration option for raw_occurrences dagster asset - ([83e968d](https://github.com/coding-kelps/edelweiss/commit/83e968da30a55616a0702ac236aa4febdebdb486))
- Add row_count of row missing vernacular name - ([b9e248e](https://github.com/coding-kelps/edelweiss/commit/b9e248e1c1d97b5e4b50f5ef6257522dfcf5e8b4))
- Add systematic preview at end of each dagster assets - ([e028c7c](https://github.com/coding-kelps/edelweiss/commit/e028c7c77633d0022abb12af4ef0478ca659a841))
- Add small dagster project with asset draft to load french alps animal species - ([912069e](https://github.com/coding-kelps/edelweiss/commit/912069eb320d1818cda71b274303a87f233b005a))
- Add hugging face logging and draft push - ([1a06bfc](https://github.com/coding-kelps/edelweiss/commit/1a06bfc8368a6df1df3cffc18804c0f39fe01318))
- Add example of GBIF querying - ([b72f3a9](https://github.com/coding-kelps/edelweiss/commit/b72f3a986b1439b335195509284ad707f347c44d))

### 🐛 Bug Fixes

- Define raw_occurrences table rather than relying on DataFrame (it can cause some problems as the DataFrame guess a column type when all entry are NULL - ([a230462](https://github.com/coding-kelps/edelweiss/commit/a2304621cafdfdcce105cc85bf5b70c5ab084fa6))

### 🚜 Refactor

- *(assets)* Change assets output from duckdb Table to parquet files - ([641d685](https://github.com/coding-kelps/edelweiss/commit/641d6853c02f5e5dec1ab27a167ba3482a4d3c4b))
- *(assets)* Change GBIF occurrences loading from duckdb to parquets - ([14a264c](https://github.com/coding-kelps/edelweiss/commit/14a264c47f26a234c1532bc479e81af46e0d6646))
- *(assets)* Rename duckdb table column names from camelCase to snake_case - ([5dfb535](https://github.com/coding-kelps/edelweiss/commit/5dfb5356f041b140102763457df96c1dc2eb591f))
- Change repository.py into defintions.py - ([3e2d0f2](https://github.com/coding-kelps/edelweiss/commit/3e2d0f2bb9c654df6ffd9d098cb7b6e6c5c5f290))
- Go back to static partioning - ([be93f6a](https://github.com/coding-kelps/edelweiss/commit/be93f6ae1052921cf171e8a3623027888efc5bc8))
- Refactor GBIF download assets with dynamic partitioning - ([bb39f55](https://github.com/coding-kelps/edelweiss/commit/bb39f55f3f67af8b5acda365cf28138c9fd79d3b))
- Put all keyword in SQL queries into capital - ([ba79803](https://github.com/coding-kelps/edelweiss/commit/ba7980342ea86e2b3d942bd3538d5970b88cf8ce))
- Refactor vernacular_name_map asset avoiding useless pandas conversion for optimization purpose - ([c057e6a](https://github.com/coding-kelps/edelweiss/commit/c057e6a85fb158b4b4ac9a120be743d590f341f0))
- Remove obsolete conversion to pandas DataFrame when it can be replaced by DuckDB SQL query directly - ([3665019](https://github.com/coding-kelps/edelweiss/commit/3665019c1fcc605d57408e189ddbfd2918ce5356))
- Separate the dagster species into 6 different assets - ([d7ad84d](https://github.com/coding-kelps/edelweiss/commit/d7ad84d272c22899fe8f5df4bac26c9a17ed75d4))
- Separate the dagster species into 6 different assets - ([39504bc](https://github.com/coding-kelps/edelweiss/commit/39504bcafa9390fa39f79663983bf6c6900f8d19))
- Change example - ([50da403](https://github.com/coding-kelps/edelweiss/commit/50da40311f536f137e11d17afbeb15dd9cb6472d))

### 🔨 Build

- *(dockerfile)* Bump to newer image of python:3.12.X - ([1ebc6bf](https://github.com/coding-kelps/edelweiss/commit/1ebc6bf0950ccd32bca24125aa1b95379cb485cd))

### ⚙️ Miscellaneous Tasks

- *(changelog)* Change git cliff configuration - ([9af7f23](https://github.com/coding-kelps/edelweiss/commit/9af7f23bce839b653e9fc962171ade40940cb3ad))
- *(dev)* Add tegola example docker compose configuration - ([92c8c5e](https://github.com/coding-kelps/edelweiss/commit/92c8c5e93c6cec229654314d6d5df06ad6b6dac3))
- *(release)* Add automated release creation with changelog auto update - ([17ca569](https://github.com/coding-kelps/edelweiss/commit/17ca569c842df24dead918fda1af30af500b8738))
- *(tegola)* Add environment variables loading in configuration - ([6e8f2b0](https://github.com/coding-kelps/edelweiss/commit/6e8f2b0f2f7eca165d0717368988336fc64ce925))
- Add development containers to .gitignore - ([7f2b670](https://github.com/coding-kelps/edelweiss/commit/7f2b6704623e65f46fa4d830e2b735226d09f264))
- Delete .DS_Store - ([92dd0bd](https://github.com/coding-kelps/edelweiss/commit/92dd0bdf028fd31f8a6f825d33c6f690267daa8c))

## New Contributors ❤️

* @guilhem-sante made their first contribution

<!-- generated by git-cliff -->
