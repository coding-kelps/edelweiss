import dagster as dg
from dagster_duckdb import DuckDBResource
from dagster import StaticPartitionsDefinition, AssetExecutionContext
from concurrent.futures import ThreadPoolExecutor, as_completed
from edelweiss.resources import GBIFAPIResource
from edelweiss.constants import OUTPUT_DIR
import zipfile
import tempfile
import os

PARTITIONED_RAW_OCCURRENCES_DIR=f"{OUTPUT_DIR}/partioned-raw-occurrences"

gbif_downloads_yearly_partitions_def = StaticPartitionsDefinition([
    "2015", "2016", "2017", "2018", "2019", "2020",
    "2021", "2022", "2023", "2024", "2025"
])

@dg.asset(
    kinds={"python", "duckdb"},
    partitions_def=gbif_downloads_yearly_partitions_def,
    group_name="ingestion",
    code_version="0.1.0",
    description="""
        Download raw observation occurences of animal species in the French Alps from the GBIF API.
    """,
    tags = {"gbif": ""},
)
def partitioned_raw_occurrences(context: AssetExecutionContext, gbif: GBIFAPIResource, duckdb: DuckDBResource) -> dg.MaterializeResult:
    year = context.partition_key
    queries = {
        "country": "FR",
        "geometry": "POLYGON((4.63127 44.84424,7.5505 44.84424,7.5505 46.81983,4.63127 46.81983,4.63127 44.84424))",
        "kingdomKey": "1",
        "year": year
    }
    
    key = gbif.request_download(queries=queries)

    downloaded_archive_path = gbif.get_download(key=key)
    context.log.info(f"downloaded GBIF archive at {downloaded_archive_path}")

    with zipfile.ZipFile(downloaded_archive_path, 'r') as z:
        member_name = z.namelist()[0]

        with z.open(member_name) as src, tempfile.NamedTemporaryFile(delete=False, suffix=f"-raw-occurrences-{key}.csv", dir=OUTPUT_DIR) as tmp:
            tmp.write(src.read())
            tmp_path = tmp.name

    with duckdb.get_connection() as conn:
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS yearly_raw_occurrences (
                gbif_id VARCHAR PRIMARY KEY,
                dataset_key VARCHAR,
                occurrence_id VARCHAR,
                kingdom VARCHAR,
                phylum VARCHAR,
                "class" VARCHAR,
                "order" VARCHAR,
                family VARCHAR,
                genus VARCHAR,
                species VARCHAR,
                infraspecific_epithet VARCHAR,
                taxon_rank VARCHAR,
                scientific_name VARCHAR,
                verbatim_scientific_name VARCHAR,
                verbatim_scientific_name_authorship VARCHAR,
                country_code VARCHAR,
                locality VARCHAR,
                state_province VARCHAR,
                occurrence_status VARCHAR,
                individual_count DOUBLE,
                publishing_org_key VARCHAR,
                decimal_latitude DOUBLE,
                decimal_longitude DOUBLE,
                coordinate_uncertainty_in_meters DOUBLE,
                coordinate_precision DOUBLE,
                elevation DOUBLE,
                elevation_accuracy DOUBLE,
                depth DOUBLE,
                depth_accuracy DOUBLE,
                event_date VARCHAR,
                day BIGINT,
                month BIGINT,
                year BIGINT,
                taxon_key BIGINT,
                species_key DOUBLE,
                basis_of_record VARCHAR,
                institution_code VARCHAR,
                collection_code VARCHAR,
                catalog_number VARCHAR,
                record_number VARCHAR,
                identified_by VARCHAR,
                date_identified VARCHAR,
                license VARCHAR,
                rights_holder VARCHAR,
                recorded_by VARCHAR,
                type_status VARCHAR,
                establishment_means VARCHAR,
                last_interpreted VARCHAR,
                media_type VARCHAR,
                issue VARCHAR
            );
        """)

        os.makedirs(PARTITIONED_RAW_OCCURRENCES_DIR, exist_ok=True)

        conn.execute(f"""
            COPY yearly_raw_occurrences
            FROM '{tmp_path}' (DELIMITER '\t', HEADER TRUE, AUTO_DETECT FALSE)
        """)

        conn.execute(f"""
            COPY yearly_raw_occurrences
            TO '{PARTITIONED_RAW_OCCURRENCES_DIR}/{key}.parquet' (FORMAT PARQUET)
        """)

        row_count = conn.execute("SELECT COUNT(*) FROM yearly_raw_occurrences").fetchone()
        count = row_count[0] if row_count else 0
    
    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(count),
        }
    )

@dg.asset(
    kinds={"python", "duckdb"},
    group_name="ingestion",
    code_version="0.11.0",
    description="""
        Combine all yearly raw occurrences into one dataset.
    """,
    tags = {},
    deps=[partitioned_raw_occurrences]
)
def raw_occurrences(context: AssetExecutionContext, duckdb: DuckDBResource) -> dg.MaterializeResult:
    with duckdb.get_connection() as conn:
        conn.execute(f"""
            CREATE TABLE raw_occurrences AS
            SELECT * FROM read_parquet('{PARTITIONED_RAW_OCCURRENCES_DIR}/*')
        """)

        conn.execute(f"""
            COPY raw_occurrences
            TO '{OUTPUT_DIR}/raw-occurrences.parquet' (FORMAT PARQUET);
        """)

        row_count = conn.execute("SELECT COUNT(*) FROM raw_occurrences").fetchone()
        count = row_count[0] if row_count else 0

    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(count),
        }
    )

@dg.asset(
    kinds={"python", "duckdb"},
    group_name="ingestion",
    code_version="0.1.0",
    description="""
    """,
    deps=[raw_occurrences]
)
def filtered_occurrences(context: AssetExecutionContext, duckdb: DuckDBResource) -> dg.MaterializeResult:
    with duckdb.get_connection() as conn:
        conn.execute(f"""
            CREATE TABLE filtered_occurrences AS
                SELECT
                    gbif_id,
                    taxon_key,
                    decimal_latitude,
                    decimal_longitude,
                    coordinate_uncertainty_in_meters,
                    coordinate_precision,
                    elevation,
                    elevation_accuracy,
                    depth,
                    depth_accuracy,
                    MAKE_DATE("year", "month", "day") as "date"
                FROM read_parquet('{OUTPUT_DIR}/raw-occurrences.parquet')
                WHERE
                    species IS NOT NULL
                    AND (
                      "class" = 'Mammalia'
                      OR "class" = 'Aves'
                    );
        """)

        conn.execute(f"""
            COPY filtered_occurrences
            TO '{OUTPUT_DIR}/filtered-occurrences.parquet' (FORMAT PARQUET);
        """)

        preview_query = "SELECT * FROM filtered_occurrences LIMIT 10"
        preview_df = conn.execute(preview_query).fetchdf()
        row_count = conn.execute("SELECT COUNT(*) FROM filtered_occurrences").fetchone()
        count = row_count[0] if row_count else 0

        return dg.MaterializeResult(
            metadata={
                "row_count": dg.MetadataValue.int(count),
                "preview": dg.MetadataValue.md(preview_df.to_markdown(index=False)),
            }
        )
    
@dg.asset(
    kinds={"python", "duckdb"},
    group_name="ingestion",
    code_version="0.2.0",
    description="""
    """,
    deps=[filtered_occurrences]
)
def geospatial_occurrences(context: AssetExecutionContext, duckdb: DuckDBResource) -> dg.MaterializeResult:
    with duckdb.get_connection() as conn:
        conn.execute(f"""
            INSTALL spatial;
            LOAD spatial;
            
            CREATE TABLE geospatial_occurrences AS
            SELECT
                gbif_id,
                taxon_key,
                ST_Point(decimal_longitude, decimal_latitude) AS coordinates,
                coordinate_uncertainty_in_meters,
                coordinate_precision,
                elevation,
                elevation_accuracy,
                depth,
                depth_accuracy,
                date
            FROM read_parquet('{OUTPUT_DIR}/filtered-occurrences.parquet');
        """)

        conn.execute(f"""
            COPY geospatial_occurrences
            TO '{OUTPUT_DIR}/geospatial-occurrences.parquet' (FORMAT PARQUET);
        """)

        preview_query = "SELECT * FROM geospatial_occurrences LIMIT 10"
        preview_df = conn.execute(preview_query).fetchdf()
        row_count = conn.execute("SELECT COUNT(*) FROM geospatial_occurrences").fetchone()
        count = row_count[0] if row_count else 0

    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(count),
            "preview": dg.MetadataValue.md(preview_df.to_markdown(index=False)),
        }
    )

@dg.asset(
    kinds={"python", "duckdb"},
    group_name="ingestion",
    code_version="0.6.0",
    description="""
    """,
    deps=[filtered_occurrences]
)
def unique_taxon_keys(context: AssetExecutionContext, duckdb: DuckDBResource) -> dg.MaterializeResult:
    with duckdb.get_connection() as conn:
        conn.execute(f"""
            CREATE TABLE unique_taxon_keys AS
                SELECT DISTINCT
                    taxon_key
                FROM read_parquet('{OUTPUT_DIR}/filtered-occurrences.parquet');
        """)

        conn.execute(f"""
            COPY unique_taxon_keys
            TO '{OUTPUT_DIR}/unique-taxon-keys.parquet' (FORMAT PARQUET);
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

THREADPOOL_MAX_WORKER=10

@dg.asset(
    kinds={"python", "duckdb"},
    group_name="ingestion",
    code_version="0.7.0",
    description="""
    """,
    tags = {"gbif": ""},
    deps=[unique_taxon_keys]
)
def vernacular_names(context: AssetExecutionContext, gbif: GBIFAPIResource, duckdb: DuckDBResource) -> dg.MaterializeResult:
    with duckdb.get_connection() as conn:
        unique_keys = conn.execute(f"""
            SELECT taxon_key
            FROM read_parquet('{OUTPUT_DIR}/unique-taxon-keys.parquet');
        """).fetchall()
        unique_keys = [key[0] for key in unique_keys]

        vernacular_name_map = {}

        with ThreadPoolExecutor(max_workers=THREADPOOL_MAX_WORKER) as executor:
            future_to_key = {executor.submit(gbif.get_species_vernacular_names, key): key for key in unique_keys}
            for future in as_completed(future_to_key):
                key = future_to_key[future]
                vernacular_name_map[key] = future.result()

        data = [(key, names["deu"], names["eng"], names["fra"]) for key, names in vernacular_name_map.items()]

        conn.execute("""
            CREATE TABLE vernacular_names (
                taxon_key INTEGER PRIMARY KEY,
                vernacular_name_deu VARCHAR,
                vernacular_name_eng VARCHAR,
                vernacular_name_fra VARCHAR
            );
        """)
        conn.executemany("""
            INSERT INTO vernacular_names (taxon_key, vernacular_name_deu, vernacular_name_eng, vernacular_name_fra)
            VALUES (?, ?, ?, ?);
        """, data)

        conn.execute(f"""
            COPY vernacular_names
            TO '{OUTPUT_DIR}/vernacular-names.parquet' (FORMAT PARQUET);
        """)

        preview_query = "SELECT * FROM vernacular_names LIMIT 10"
        preview_df = conn.execute(preview_query).fetchdf()
        row_count = conn.execute("SELECT COUNT(*) FROM vernacular_names").fetchone()
        count = row_count[0] if row_count else 0

    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(count),
            "preview": dg.MetadataValue.md(preview_df.to_markdown(index=False)),
        }
    )
    
@dg.asset(
    kinds={"python", "duckdb"},
    group_name="ingestion",
    code_version="0.1.0",
    description="",
    deps=[unique_taxon_keys]
)
def scientific_names(context: AssetExecutionContext, duckdb: DuckDBResource) -> dg.MaterializeResult:
    with duckdb.get_connection() as conn:
        conn.execute(f"""
            CREATE TABLE scientific_names AS
            SELECT DISTINCT
                unique_taxon_keys.taxon_key,
                raw_occurrences.scientific_name
            FROM read_parquet('{OUTPUT_DIR}/unique-taxon-keys.parquet') AS 'unique_taxon_keys'
            JOIN read_parquet('{OUTPUT_DIR}/raw-occurrences.parquet') AS 'raw_occurrences' USING (taxon_key);
        """)

        conn.execute(f"""
            COPY scientific_names
            TO '{OUTPUT_DIR}/scientific-names.parquet' (FORMAT PARQUET);
        """)

        preview_query = "SELECT * FROM scientific_names LIMIT 10"
        preview_df = conn.execute(preview_query).fetchdf()
        row_count = conn.execute("SELECT COUNT(*) FROM scientific_names").fetchone()
        count = row_count[0] if row_count else 0

    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(count),
            "preview": dg.MetadataValue.md(preview_df.to_markdown(index=False)),
        }
    )
