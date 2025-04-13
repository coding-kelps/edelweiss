from dagster_duckdb import DuckDBResource
import dagster as dg
from dagster import StaticPartitionsDefinition
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from edelweiss.resources.gbif import GBIFAPIResource

THREADPOOL_MAX_WORKER=10

gbif_downloads_yearly_partitions_def = StaticPartitionsDefinition([
    "2015", "2016", "2017", "2018", "2019", "2020",
    "2021", "2022", "2023", "2024", "2025"
])

@dg.asset(
    compute_kind="duckdb",
    partitions_def=gbif_downloads_yearly_partitions_def,
    group_name="ingestion",
    code_version="0.3.0",
    description="",
    tags = {"gbif": ""}
)
def generated_gbif_download_keys(context, duckdb: DuckDBResource, gbif: GBIFAPIResource) -> dg.MaterializeResult:
    year = context.partition_key
    queries = {
        "country": "FR",
        "geometry": "POLYGON((4.63127 44.84424,7.5505 44.84424,7.5505 46.81983,4.63127 46.81983,4.63127 44.84424))",
        "kingdomKey": "1",
        "year": year
    }

    formated_queries = [f"{key} = {value}" for key, value in queries.items()]
    
    key = gbif.request_download(queries=formated_queries)

    with duckdb.get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS generated_gbif_download_keys (
                year INTEGER PRIMARY KEY,
                key VARCHAR
            );
        """)

        conn.execute("""
            INSERT OR REPLACE INTO generated_gbif_download_keys (year, key)
            VALUES (?, ?);
        """, (year, key))

        preview_query = "SELECT * FROM generated_gbif_download_keys LIMIT 10"
        preview_df = conn.execute(preview_query).fetchdf()
        row_count = conn.execute("SELECT COUNT(*) FROM generated_gbif_download_keys").fetchone()
        count = row_count[0] if row_count else 0

    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(count),
            "preview": dg.MetadataValue.md(preview_df.to_markdown(index=False)),
        }
    )

@dg.asset(
    compute_kind="duckdb",
    partitions_def=gbif_downloads_yearly_partitions_def,
    group_name="ingestion",
    code_version="0.8.0",
    description="Download raw observation occurences of animal species in the French Alps from a GBIF donwload key",
    tags = {"gbif": ""},
    deps=[generated_gbif_download_keys]
)
def raw_occurrences(context, gbif: GBIFAPIResource, duckdb: DuckDBResource) -> dg.MaterializeResult:
    year = context.partition_key

    with duckdb.get_connection() as conn:
        row = conn.execute(
            "SELECT key FROM generated_gbif_download_keys WHERE year = ?",
            (year,)
        ).fetchone()

        if row is None:
            raise ValueError(f"No download key found for year {year}")

        key = row[0]

    downloaded_archive_path = gbif.get_download(key=key)
    df = pd.read_csv(downloaded_archive_path, sep='\t')

    with duckdb.get_connection() as conn:
        conn.register("df_view", df)

        table_exists = conn.execute("""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_name = 'raw_occurrences'
        """).fetchone()[0]
        
        if table_exists == 0:
            # https://techdocs.gbif.org/en/data-use/download-formats
            conn.execute(f"""
                CREATE TABLE IF NOT EXISTS raw_occurrences (
                    gbifID VARCHAR,
                    datasetKey VARCHAR,
                    occurrenceID VARCHAR,
                    kingdom VARCHAR,
                    phylum VARCHAR,
                    "class" VARCHAR,
                    "order" VARCHAR,
                    family VARCHAR,
                    genus VARCHAR,
                    species VARCHAR,
                    infraspecificEpithet VARCHAR,
                    taxonRank VARCHAR,
                    scientificName VARCHAR,
                    verbatimScientificName VARCHAR,
                    verbatimScientificNameAuthorship VARCHAR,
                    countryCode VARCHAR,
                    locality VARCHAR,
                    stateProvince VARCHAR,
                    occurrenceStatus VARCHAR,
                    individualCount DOUBLE,
                    publishingOrgKey VARCHAR,
                    decimalLatitude DOUBLE,
                    decimalLongitude DOUBLE,
                    coordinateUncertaintyInMeters DOUBLE,
                    coordinatePrecision DOUBLE,
                    elevation DOUBLE,
                    elevationAccuracy DOUBLE,
                    depth DOUBLE,
                    depthAccuracy VARCHAR,
                    eventDate VARCHAR,
                    day BIGINT,
                    month BIGINT,
                    year BIGINT,
                    taxonKey BIGINT,
                    speciesKey DOUBLE,
                    basisOfRecord VARCHAR,
                    institutionCode VARCHAR,
                    collectionCode VARCHAR,
                    catalogNumber VARCHAR,
                    recordNumber VARCHAR,
                    identifiedBy VARCHAR,
                    dateIdentified VARCHAR,
                    license VARCHAR,
                    rightsHolder VARCHAR,
                    recordedBy VARCHAR,
                    typeStatus VARCHAR,
                    establishmentMeans VARCHAR,
                    lastInterpreted VARCHAR,
                    mediaType VARCHAR,
                    issue VARCHAR
                );
            """)
        else:
            conn.execute("INSERT INTO raw_occurrences SELECT * FROM df_view")

        preview_query = "SELECT * FROM raw_occurrences LIMIT 10"
        preview_df = conn.execute(preview_query).fetchdf()
        row_count = conn.execute("SELECT COUNT(*) FROM raw_occurrences").fetchone()
        count = row_count[0] if row_count else 0

    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(count),
            "preview": dg.MetadataValue.md(preview_df.to_markdown(index=False)),
        }
    )

@dg.asset(
    compute_kind="duckdb",
    group_name="ingestion",
    code_version="0.4.0",
    description="Create a new table \"pruned_occurrences\" with only revelant columns for the edelweiss preprocessing pipeline from \"raw_occurrences\"",
    deps=[raw_occurrences]
)
def pruned_occurrences(duckdb: DuckDBResource) -> dg.MaterializeResult:
    with duckdb.get_connection() as conn:
        conn.execute("""
            CREATE OR REPLACE TABLE pruned_occurrences AS
            SELECT 
                taxonKey, 
                scientificName, 
                coordinateUncertaintyInMeters, 
                decimalLatitude, 
                decimalLongitude, 
            FROM raw_occurrences;
        """)

        preview_query = "SELECT * FROM pruned_occurrences LIMIT 10"
        preview_df = conn.execute(preview_query).fetchdf()
        row_count = conn.execute("SELECT COUNT(*) FROM pruned_occurrences").fetchone()
        count = row_count[0] if row_count else 0

        return dg.MaterializeResult(
            metadata={
                "row_count": dg.MetadataValue.int(count),
                "preview": dg.MetadataValue.md(preview_df.to_markdown(index=False)),
            }
        )

@dg.asset(
    compute_kind="duckdb",
    group_name="ingestion",
    code_version="0.4.0",
    description="Create a new table \"unique_taxon_keys\" listing all unique GBIF taxon key from \"raw_occurrences\"",
    deps=[raw_occurrences]
)
def unique_taxon_keys(duckdb: DuckDBResource) -> dg.MaterializeResult:
    with duckdb.get_connection() as conn:
        conn.execute("""
            CREATE OR REPLACE TABLE unique_taxon_keys AS
            SELECT DISTINCT taxonKey FROM raw_occurrences;
        """)

        preview_query = "SELECT * FROM unique_taxon_keys LIMIT 10"
        preview_df = conn.execute(preview_query).fetchdf()
        row_count = conn.execute("SELECT COUNT(*) FROM unique_taxon_keys").fetchone()
        count = row_count[0] if row_count else 0

        return dg.MaterializeResult(
            metadata={
                "row_count": dg.MetadataValue.int(count),
                "preview": dg.MetadataValue.md(preview_df.to_markdown(index=False)),
            }
        )

@dg.asset(
    compute_kind="duckdb",
    group_name="ingestion",
    code_version="0.5.0",
    description="Create a new table \"vernacular_name_map\" mapping all vernacular name for all taxon key of \"unique_taxon_keys\"",
    tags = {"gbif": ""},
    deps=[unique_taxon_keys]
)
def vernacular_name_map(gbif: GBIFAPIResource, duckdb: DuckDBResource) -> dg.MaterializeResult:
    with duckdb.get_connection() as conn:
        unique_keys = conn.sql("SELECT taxonKey FROM unique_taxon_keys").fetchall()
        unique_keys = [key[0] for key in unique_keys]

        vernacular_name_map = {}

        with ThreadPoolExecutor(max_workers=THREADPOOL_MAX_WORKER) as executor:
            future_to_key = {executor.submit(gbif.get_species_info, key): key for key in unique_keys}
            for future in as_completed(future_to_key):
                key = future_to_key[future]
                vernacular_name_map[key] = future.result()

        data = [(key, name) for key, name in vernacular_name_map.items()]

        conn.execute("""
            CREATE TABLE IF NOT EXISTS vernacular_name_map (
                taxonKey INTEGER PRIMARY KEY,
                vernacularName VARCHAR
            );
        """)
        conn.executemany("""
            INSERT OR REPLACE INTO vernacular_name_map (taxonKey, vernacularName)
            VALUES (?, ?);
        """, data)

        preview_query = "SELECT * FROM vernacular_name_map LIMIT 10"
        preview_df = conn.execute(preview_query).fetchdf()
        row_count = conn.execute("SELECT COUNT(*) FROM vernacular_name_map").fetchone()
        count = row_count[0] if row_count else 0
        missing_vernacular_row_count = conn.execute("SELECT COUNT(*) FROM vernacular_name_map WHERE vernacularName IS NULL").fetchone()
        missing_vernacular_count = missing_vernacular_row_count[0] if missing_vernacular_row_count else 0

        return dg.MaterializeResult(
            metadata={
                "row_count": dg.MetadataValue.int(count),
                "preview": dg.MetadataValue.md(preview_df.to_markdown(index=False)),
                "missing_vernacular_row_count": dg.MetadataValue.int(missing_vernacular_count)
            }
        )

@dg.asset(
    compute_kind="duckdb",
    group_name="ingestion",
    code_version="0.6.0",
    description="Create a new table \"vernacular_name_mapped_occurrences\" mapping all vernacular name for each row of \"pruned_occurrences\" from \"vernacular_name_map\"",
    deps=[vernacular_name_map, pruned_occurrences]
)
def vernacular_name_mapped_occurrences(duckdb: DuckDBResource) -> dg.MaterializeResult:
    with duckdb.get_connection() as conn:
        conn.execute("""
            CREATE OR REPLACE TABLE vernacular_name_mapped_occurrences AS
            SELECT 
                p.*, 
                v.vernacularName
            FROM pruned_occurrences p
            LEFT JOIN vernacular_name_map v
            ON p.taxonKey = v.taxonKey;
        """)

        preview_query = "SELECT * FROM vernacular_name_mapped_occurrences LIMIT 10"
        preview_df = conn.execute(preview_query).fetchdf()
        row_count = conn.execute("SELECT COUNT(*) FROM vernacular_name_mapped_occurrences").fetchone()
        count = row_count[0] if row_count else 0
        missing_vernacular_row_count = conn.execute("SELECT COUNT(*) FROM vernacular_name_mapped_occurrences WHERE vernacularName IS NULL").fetchone()
        missing_vernacular_count = missing_vernacular_row_count[0] if missing_vernacular_row_count else 0

        return dg.MaterializeResult(
            metadata={
                "row_count": dg.MetadataValue.int(count),
                "preview": dg.MetadataValue.md(preview_df.to_markdown(index=False)),
                "missing_vernacular_row_count": dg.MetadataValue.int(missing_vernacular_count)
            }
        )
