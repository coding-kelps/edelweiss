# Changelog

All notable changes to this project will be documented in this file.

## [0.0.1] - 2025-05-27

### ⛰️  Features

- *(assets)* Add date to occurrences - ([d0fe8ee](https://github.com/coding-kelps/edelweiss/commit/d0fe8ee8e66e97114160b652da891e50e38a96a5))
- *(assets)* Add species scientific name to operational database - ([911430a](https://github.com/coding-kelps/edelweiss/commit/911430aece48f9d7fb6dc90e3abaad8d02e526ae))
- *(assets)* Add two new assets species_names and occurrences to load more information to operational database - ([658b78b](https://github.com/coding-kelps/edelweiss/commit/658b78b33171092245d67609cef4aed0df45adfb))
- *(assets)* Add new asset "species_occurrences" to load occurrences spatial multipoints to PostgreSQL - ([6139802](https://github.com/coding-kelps/edelweiss/commit/6139802e85d67ad4f0996249d0f3e6615f742f91))
- *(assets)* Add lat long values convertion to geospatial data structure with newly added geospatial_occurrences and occurrences_by_taxon_key tables - ([518d474](https://github.com/coding-kelps/edelweiss/commit/518d474334b5b76ad8757ec7658da50a51df3245))
- *(resources)* Download GBIF dataset in temporary directories - ([5daac8e](https://github.com/coding-kelps/edelweiss/commit/5daac8ee29d23cff893461b5c089dfe57cdbb1ce))
- *(tegola)* Add taxon_key filter to display only one species in the map - ([6b3588e](https://github.com/coding-kelps/edelweiss/commit/6b3588e2d805512afb0d6d0e2f1b88a024f47b8a))
- Separate data ingestion from operational loading - ([6e8676b](https://github.com/coding-kelps/edelweiss/commit/6e8676bb83f113b0d9bef2f69758bf4434afefa8))
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

- *(assets)* Change assets output from duckdb Table to parquet files - ([ccadd60](https://github.com/coding-kelps/edelweiss/commit/ccadd60a9995763c85ccf5791a21b3aa3a7bdd7c))
- *(assets)* Change GBIF occurrences loading from duckdb to parquets - ([ace0047](https://github.com/coding-kelps/edelweiss/commit/ace0047b801335e41dfd72e946c9852156c72d0f))
- *(assets)* Rename duckdb table column names from camelCase to snake_case - ([0771743](https://github.com/coding-kelps/edelweiss/commit/0771743fa59d8aab87518a4b3c53091dac9aea9a))
- Change repository.py into defintions.py - ([6fee3e1](https://github.com/coding-kelps/edelweiss/commit/6fee3e1e783c2aae8839bb4844077f51695865ca))
- Go back to static partioning - ([be93f6a](https://github.com/coding-kelps/edelweiss/commit/be93f6ae1052921cf171e8a3623027888efc5bc8))
- Refactor GBIF download assets with dynamic partitioning - ([bb39f55](https://github.com/coding-kelps/edelweiss/commit/bb39f55f3f67af8b5acda365cf28138c9fd79d3b))
- Put all keyword in SQL queries into capital - ([ba79803](https://github.com/coding-kelps/edelweiss/commit/ba7980342ea86e2b3d942bd3538d5970b88cf8ce))
- Refactor vernacular_name_map asset avoiding useless pandas conversion for optimization purpose - ([c057e6a](https://github.com/coding-kelps/edelweiss/commit/c057e6a85fb158b4b4ac9a120be743d590f341f0))
- Remove obsolete conversion to pandas DataFrame when it can be replaced by DuckDB SQL query directly - ([3665019](https://github.com/coding-kelps/edelweiss/commit/3665019c1fcc605d57408e189ddbfd2918ce5356))
- Separate the dagster species into 6 different assets - ([d7ad84d](https://github.com/coding-kelps/edelweiss/commit/d7ad84d272c22899fe8f5df4bac26c9a17ed75d4))
- Separate the dagster species into 6 different assets - ([39504bc](https://github.com/coding-kelps/edelweiss/commit/39504bcafa9390fa39f79663983bf6c6900f8d19))
- Change example - ([50da403](https://github.com/coding-kelps/edelweiss/commit/50da40311f536f137e11d17afbeb15dd9cb6472d))

### ⚙️ Miscellaneous Tasks

- *(release)* Add automated release creation with changelog auto update - ([b856441](https://github.com/coding-kelps/edelweiss/commit/b8564413ea55d63c3b7087e1aafc06c86978a4a6))
- Delete .DS_Store - ([92dd0bd](https://github.com/coding-kelps/edelweiss/commit/92dd0bdf028fd31f8a6f825d33c6f690267daa8c))

### Build

- *(dockerfile)* Bump to newer image of python:3.12.X - ([273c4ad](https://github.com/coding-kelps/edelweiss/commit/273c4ad53ee9962156a68430ebbc843aaf9b9839))

### Dev

- *(tegola)* Add environment variables loading in configuration - ([f44258a](https://github.com/coding-kelps/edelweiss/commit/f44258a1aabe09aaf3d1c660ac5cb6860eedf8f3))
- Add tegola example docker compose configuration - ([d7ce2b1](https://github.com/coding-kelps/edelweiss/commit/d7ce2b16bea2a0a34b90b73144421a8cf354072a))

## New Contributors ❤️

* @guilhem-sante made their first contribution

<!-- generated by git-cliff -->
