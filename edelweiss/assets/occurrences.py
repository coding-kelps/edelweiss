from dagster_duckdb import DuckDBResource
import dagster as dg
from dagster import StaticPartitionsDefinition, AssetExecutionContext
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from edelweiss.resources.gbif import GBIFAPIResource
from edelweiss.resources.postgresql import PostgreSQLResource
import psycopg
from edelweiss.constants import OUTPUT_DIR
import zipfile
import tempfile
import os

THREADPOOL_MAX_WORKER=10
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
    code_version="0.7.0",
    description="Create a new table \"pruned_occurrences\" with only revelant columns for the edelweiss preprocessing pipeline from \"raw_occurrences\"",
    deps=[raw_occurrences]
)
def pruned_occurrences(context: AssetExecutionContext, duckdb: DuckDBResource) -> dg.MaterializeResult:
    with duckdb.get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS pruned_occurrences (
                gbif_id VARCHAR PRIMARY KEY,
                taxon_key BIGINT,
                decimal_latitude DOUBLE,
                decimal_longitude DOUBLE,
                coordinate_uncertainty_in_meters DOUBLE,
                coordinate_precision DOUBLE,
                elevation DOUBLE,
                elevation_accuracy DOUBLE,
                depth DOUBLE,
                depth_accuracy DOUBLE,
                date DATE
            );
        """)
        conn.execute("""
            INSERT INTO pruned_occurrences (
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
                "date"
            )
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
                MAKE_DATE("year", "month", "day")
            FROM '
            WHERE
                species IS NOT NULL
                AND (
                  "class" = 'Mammalia'
                  OR "class" = 'Aves'
                );
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
    kinds={"python", "duckdb"},
    group_name="ingestion",
    code_version="0.2.0",
    description="""
        Create a new table \"geospatial_occurrences\" from \"pruned_occurrences\" converting all latitude
        and longitude values to a single geospatial point
    """,
    deps=[pruned_occurrences]
)
def geospatial_occurrences(context: AssetExecutionContext, duckdb: DuckDBResource) -> dg.MaterializeResult:
    with duckdb.get_connection() as conn:
        conn.execute("""
            INSTALL spatial;
            LOAD spatial;
            
            CREATE OR REPLACE TABLE geospatial_occurrences AS
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
            FROM pruned_occurrences;
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
    description="Create a new table \"unique_taxon_keys\" listing all unique GBIF taxon key from \"raw_occurrences\"",
    deps=[pruned_occurrences]
)
def unique_taxon_keys(context: AssetExecutionContext, duckdb: DuckDBResource) -> dg.MaterializeResult:
    with duckdb.get_connection() as conn:
        conn.execute("""
            CREATE OR REPLACE TABLE unique_taxon_keys AS
            SELECT DISTINCT taxon_key FROM pruned_occurrences;
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
    kinds={"python", "duckdb"},
    group_name="ingestion",
    code_version="0.7.0",
    description="Create a new table \"vernacular_name_map\" mapping all vernacular name for all taxon key of \"unique_taxon_keys\"",
    tags = {"gbif": ""},
    deps=[unique_taxon_keys]
)
def vernacular_names(context: AssetExecutionContext, gbif: GBIFAPIResource, duckdb: DuckDBResource) -> dg.MaterializeResult:
    with duckdb.get_connection() as conn:
        unique_keys = conn.sql("SELECT taxon_key FROM unique_taxon_keys").fetchall()
        unique_keys = [key[0] for key in unique_keys]

        vernacular_name_map = {}

        with ThreadPoolExecutor(max_workers=THREADPOOL_MAX_WORKER) as executor:
            future_to_key = {executor.submit(gbif.get_species_vernacular_names, key): key for key in unique_keys}
            for future in as_completed(future_to_key):
                key = future_to_key[future]
                vernacular_name_map[key] = future.result()

        data = [(key, names["deu"], names["eng"], names["fra"]) for key, names in vernacular_name_map.items()]

        conn.execute("""
            CREATE TABLE IF NOT EXISTS vernacular_names (
                taxon_key INTEGER PRIMARY KEY,
                vernacular_name_deu VARCHAR,
                vernacular_name_eng VARCHAR,
                vernacular_name_fra VARCHAR,
            );
        """)
        conn.executemany("""
            INSERT OR REPLACE INTO vernacular_names (taxon_key, vernacular_name_deu, vernacular_name_eng, vernacular_name_fra)
            VALUES (?, ?, ?, ?);
        """, data)

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
def scientific_names(context: AssetExecutionContext, duckdb: DuckDBResource, postgresql: PostgreSQLResource) -> dg.MaterializeResult:
    with duckdb.get_connection() as conn:
        conn.execute("""
            INSTALL spatial;
            LOAD spatial;
            
            CREATE OR REPLACE TABLE scientific_names AS
            SELECT DISTINCT
                unique_taxon_keys.taxon_key,
                raw_occurrences.scientific_name
            FROM unique_taxon_keys
            JOIN raw_occurrences USING (taxon_key);
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


@dg.asset(
    kinds={"python", "duckdb", "postgresql"},
    group_name="ingestion",
    code_version="0.1.0",
    description="",
    deps=[geospatial_occurrences]
)
def species_all_occurrence(context: AssetExecutionContext, duckdb: DuckDBResource, postgresql: PostgreSQLResource) -> dg.MaterializeResult:
    with duckdb.get_connection() as conn:
        result = conn.execute("""
            INSTALL spatial;
            LOAD spatial;
                              
            SELECT
                taxon_key,
                ST_AsWKB(ST_Collect(list(coordinates))) AS wkb
            FROM geospatial_occurrences
            GROUP BY taxon_key;
        """).fetch_arrow_table()

    data = [
        (int(row["taxon_key"]), psycopg.Binary(row["wkb"]))
        for row in result.to_pylist()
    ]
    
    with postgresql.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS species_all_occurrence (
                    taxon_key INTEGER PRIMARY KEY,
                    all_occurrence_coordinates geometry(MultiPoint, 4326)
                );
            """)

            cur.executemany("""
                INSERT INTO species_all_occurrence (
                    taxon_key,
                    all_occurrence_coordinates
                )
                VALUES (
                    %s,
                    ST_GeomFromWKB(%s)
                )
                ON CONFLICT (taxon_key) DO UPDATE
                SET
                    all_occurrence_coordinates = EXCLUDED.all_occurrence_coordinates;
            """, data)

            cur.execute("SELECT * FROM species_all_occurrence LIMIT 10;")
            cols = [col.name for col in cur.description]
            rows = cur.fetchall()
            preview_df = pd.DataFrame(rows, columns=cols)
            row_count = conn.execute("SELECT COUNT(*) FROM species_all_occurrence").fetchone()
            count = row_count[0] if row_count else 0
        
        conn.commit()

    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(count),
            "preview": dg.MetadataValue.md(preview_df.to_markdown(index=False)),
        }
    )

@dg.asset(
    kinds={"python", "duckdb", "postgresql"},
    group_name="ingestion",
    code_version="0.1.0",
    description="",
    deps=[vernacular_names, scientific_names]
)
def species_names(context: AssetExecutionContext, duckdb: DuckDBResource, postgresql: PostgreSQLResource) -> dg.MaterializeResult:
    with duckdb.get_connection() as conn:
        result = conn.execute("""
            SELECT
                scientific_names.taxon_key,
                scientific_names.scientific_name,
                vernacular_names.vernacular_name_deu,
                vernacular_names.vernacular_name_eng,
                vernacular_names.vernacular_name_fra
            FROM scientific_names
            JOIN vernacular_names USING (taxon_key);
        """).fetch_arrow_table()

    data = [
        (int(row["taxon_key"]), row["scientific_name"], row["vernacular_name_deu"], row["vernacular_name_eng"], row["vernacular_name_fra"])
        for row in result.to_pylist()
    ]

    with postgresql.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS species_names (
                    taxon_key INTEGER PRIMARY KEY,
                    scientific_name VARCHAR,
                    vernacular_name_deu VARCHAR,
                    vernacular_name_eng VARCHAR,
                    vernacular_name_fra VARCHAR
                );
            """)

            cur.executemany("""
                INSERT INTO species_names (
                    taxon_key,
                    scientific_name,
                    vernacular_name_deu,
                    vernacular_name_eng,
                    vernacular_name_fra
                )
                VALUES (
                    %s,
                    %s,
                    %s,
                    %s,
                    %s
                )
                ON CONFLICT (taxon_key) DO UPDATE
                SET
                    scientific_name = EXCLUDED.scientific_name,
                    vernacular_name_deu = EXCLUDED.vernacular_name_deu,
                    vernacular_name_eng = EXCLUDED.vernacular_name_eng,
                    vernacular_name_fra = EXCLUDED.vernacular_name_fra;
            """, data)
        
            cur.execute("SELECT * FROM species_names LIMIT 10;")
            cols = [col.name for col in cur.description]
            rows = cur.fetchall()
            preview_df = pd.DataFrame(rows, columns=cols)
            row_count = conn.execute("SELECT COUNT(*) FROM species_names").fetchone()
            count = row_count[0] if row_count else 0

    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(count),
            "preview": dg.MetadataValue.md(preview_df.to_markdown(index=False)),
        }
    )

@dg.asset(
    kinds={"python", "duckdb", "postgresql"},
    group_name="ingestion",
    code_version="0.2.0",
    description="",
    deps=[geospatial_occurrences]
)
def occurrences(context: AssetExecutionContext, duckdb: DuckDBResource, postgresql: PostgreSQLResource) -> dg.MaterializeResult:
    with duckdb.get_connection() as conn:
        result = conn.execute("""
            INSTALL spatial;
            LOAD spatial;

            SELECT
                gbif_id,
                taxon_key,
                ST_AsWKB(coordinates) AS wkb,
                coordinate_uncertainty_in_meters,
                coordinate_precision,
                elevation,
                elevation_accuracy,
                depth,
                depth_accuracy,
                date
            FROM geospatial_occurrences;
        """).fetch_arrow_table()

    data = [
        (
            row["gbif_id"],
            int(row["taxon_key"]),
            psycopg.Binary(row["wkb"]),
            float(row["coordinate_uncertainty_in_meters"]) if row["coordinate_uncertainty_in_meters"] else None,
            float(row["coordinate_precision"]) if row["coordinate_precision"] else None,
            float(row["elevation"]) if row["elevation"] else None,
            float(row["elevation_accuracy"]) if row["elevation_accuracy"] else None,
            float(row["depth"]) if row["depth"] else None,
            float(row["depth_accuracy"]) if row["depth_accuracy"] else None,
            row["date"]
        )
        for row in result.to_pylist()
    ]

    with postgresql.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS occurrences (
                    gbif_id VARCHAR PRIMARY KEY,
                    taxon_key INTEGER,
                    coordinates geometry(Point, 4326),
                    coordinate_uncertainty_in_meters DOUBLE PRECISION,
                    coordinate_precision DOUBLE PRECISION,
                    elevation DOUBLE PRECISION,
                    elevation_accuracy DOUBLE PRECISION,
                    depth DOUBLE PRECISION,
                    depth_accuracy DOUBLE PRECISION,
                    "date" date
                );
            """)

            cur.executemany("""
                INSERT INTO occurrences (
                    gbif_id,
                    taxon_key,
                    coordinates,
                    coordinate_uncertainty_in_meters,
                    coordinate_precision,
                    elevation,
                    elevation_accuracy,
                    depth,
                    depth_accuracy,
                    "date"
                )
                VALUES (
                    %s,
                    %s,
                    ST_GeomFromWKB(%s),
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s
                )
                ON CONFLICT (gbif_id) DO UPDATE
                SET
                    taxon_key = EXCLUDED.taxon_key,
                    coordinates = EXCLUDED.coordinates,
                    coordinate_uncertainty_in_meters = EXCLUDED.coordinate_uncertainty_in_meters,
                    coordinate_precision = EXCLUDED.coordinate_precision,
                    elevation = EXCLUDED.elevation,
                    elevation_accuracy = EXCLUDED.elevation_accuracy,
                    depth = EXCLUDED.depth,
                    depth_accuracy = EXCLUDED.depth_accuracy,
                    "date" = EXCLUDED."date";
            """, data)

            cur.execute("SELECT * FROM occurrences LIMIT 10;")
            cols = [col.name for col in cur.description]
            rows = cur.fetchall()
            preview_df = pd.DataFrame(rows, columns=cols)
            row_count = conn.execute("SELECT COUNT(*) FROM occurrences").fetchone()
            count = row_count[0] if row_count else 0
        
        conn.commit()

    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(count),
            "preview": dg.MetadataValue.md(preview_df.to_markdown(index=False)),
        }
    )
