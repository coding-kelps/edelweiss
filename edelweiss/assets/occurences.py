from dagster_duckdb import DuckDBResource
import dagster as dg
from dagster import StaticPartitionsDefinition, AssetExecutionContext
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from edelweiss.resources.gbif import GBIFAPIResource
from edelweiss.resources.postgresql import PostgreSQLResource
import psycopg

THREADPOOL_MAX_WORKER=10

gbif_downloads_yearly_partitions_def = StaticPartitionsDefinition([
    "2015", "2016", "2017", "2018", "2019", "2020",
    "2021", "2022", "2023", "2024", "2025"
])

@dg.asset(
    kinds={"python", "duckdb"},
    partitions_def=gbif_downloads_yearly_partitions_def,
    group_name="ingestion",
    code_version="0.5.0",
    description="",
    tags = {"gbif": ""}
)
def generated_gbif_download_keys(context: AssetExecutionContext, duckdb: DuckDBResource, gbif: GBIFAPIResource) -> dg.MaterializeResult:
    year = context.partition_key
    queries = {
        "country": "FR",
        "geometry": "POLYGON((4.63127 44.84424,7.5505 44.84424,7.5505 46.81983,4.63127 46.81983,4.63127 44.84424))",
        "kingdomKey": "1",
        "year": year
    }
    
    key = gbif.request_download(queries=queries)

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
    kinds={"python", "duckdb"},
    partitions_def=gbif_downloads_yearly_partitions_def,
    group_name="ingestion",
    code_version="0.9.0",
    description="Download raw observation occurences of animal species in the French Alps from a GBIF donwload key",
    tags = {"gbif": ""},
    deps=[generated_gbif_download_keys]
)
def raw_occurrences(context: AssetExecutionContext, gbif: GBIFAPIResource, duckdb: DuckDBResource) -> dg.MaterializeResult:
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
                    depth_accuracy VARCHAR,
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
    kinds={"python", "duckdb"},
    group_name="ingestion",
    code_version="0.6.0",
    description="Create a new table \"pruned_occurrences\" with only revelant columns for the edelweiss preprocessing pipeline from \"raw_occurrences\"",
    deps=[raw_occurrences]
)
def pruned_occurrences(context: AssetExecutionContext, duckdb: DuckDBResource) -> dg.MaterializeResult:
    with duckdb.get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS pruned_occurrences (
                gbif_id VARCHAR PRIMARY KEY,
                taxon_key BIGINT,
                scientific_name VARCHAR,
                decimal_latitude DOUBLE,
                decimal_longitude DOUBLE,
                coordinate_uncertainty_in_meters DOUBLE,
                coordinate_precision DOUBLE,
                elevation DOUBLE,
                elevation_accuracy DOUBLE,
                depth DOUBLE,
                depth_accuracy VARCHAR,
                day BIGINT,
                month BIGINT,
                year BIGINT
            );
        """)
        conn.execute("""
            INSERT INTO pruned_occurrences (
                gbif_id,
                taxon_key,
                scientific_name,
                coordinate_uncertainty_in_meters,
                decimal_latitude,
                decimal_longitude
            )
            SELECT
                gbif_id,
                taxon_key,
                scientific_name,
                coordinate_uncertainty_in_meters,
                decimal_latitude,
                decimal_longitude
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
    kinds={"python", "duckdb"},
    group_name="ingestion",
    code_version="0.1.0",
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
                scientific_name,
                coordinate_uncertainty_in_meters,
                ST_Point(decimal_longitude, decimal_latitude) AS coordinates
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
    code_version="0.5.0",
    description="Create a new table \"unique_taxon_keys\" listing all unique GBIF taxon key from \"raw_occurrences\"",
    deps=[raw_occurrences]
)
def unique_taxon_keys(context: AssetExecutionContext, duckdb: DuckDBResource) -> dg.MaterializeResult:
    with duckdb.get_connection() as conn:
        conn.execute("""
            CREATE OR REPLACE TABLE unique_taxon_keys AS
            SELECT DISTINCT taxon_key FROM raw_occurrences;
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
def vernacular_name_map(context: AssetExecutionContext, gbif: GBIFAPIResource, duckdb: DuckDBResource) -> dg.MaterializeResult:
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
            CREATE TABLE IF NOT EXISTS vernacular_name_map (
                taxon_key INTEGER PRIMARY KEY,
                vernacular_name_deu VARCHAR,
                vernacular_name_eng VARCHAR,
                vernacular_name_fra VARCHAR,
            );
        """)
        conn.executemany("""
            INSERT OR REPLACE INTO vernacular_name_map (taxon_key, vernacular_name_deu, vernacular_name_eng, vernacular_name_fra)
            VALUES (?, ?, ?, ?);
        """, data)

        preview_query = "SELECT * FROM vernacular_name_map LIMIT 10"
        preview_df = conn.execute(preview_query).fetchdf()
        row_count = conn.execute("SELECT COUNT(*) FROM vernacular_name_map").fetchone()
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
    code_version="0.8.0",
    description="Create a new table \"vernacular_name_mapped_occurrences\" mapping all vernacular name for each row of \"geospatial_occurrences\" from \"vernacular_name_map\"",
    deps=[vernacular_name_map, geospatial_occurrences]
)
def vernacular_name_mapped_occurrences(context: AssetExecutionContext, duckdb: DuckDBResource) -> dg.MaterializeResult:
    with duckdb.get_connection() as conn:
        conn.execute("""
            CREATE OR REPLACE TABLE vernacular_name_mapped_occurrences AS
            SELECT 
                g.*, 
                v.vernacular_name_deu,
                v.vernacular_name_eng,
                v.vernacular_name_fra
            FROM geospatial_occurrences g
            LEFT JOIN vernacular_name_map v
            ON g.taxon_key = v.taxon_key;
        """)

        preview_query = "SELECT * FROM vernacular_name_mapped_occurrences LIMIT 10"
        preview_df = conn.execute(preview_query).fetchdf()
        row_count = conn.execute("SELECT COUNT(*) FROM vernacular_name_mapped_occurrences").fetchone()
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
    description="Create a new table \"occurrences_by_taxon_key\" grouping all occurrences by taxon key",
    deps=[unique_taxon_keys, geospatial_occurrences]
)
def occurrences_by_taxon_key(context: AssetExecutionContext, duckdb: DuckDBResource) -> dg.MaterializeResult:
    with duckdb.get_connection() as conn:
        conn.execute("""
            INSTALL spatial;
            LOAD spatial;
            
            CREATE OR REPLACE TABLE occurrences_by_taxon_key AS
            SELECT
                taxon_key,
                ST_Collect(list(coordinates)) AS occurrence_coordinates
            FROM geospatial_occurrences
            GROUP BY taxon_key;
        """)

        preview_query = "SELECT * FROM occurrences_by_taxon_key LIMIT 10"
        preview_df = conn.execute(preview_query).fetchdf()
        row_count = conn.execute("SELECT COUNT(*) FROM occurrences_by_taxon_key").fetchone()
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
    deps=[occurrences_by_taxon_key]
)
def species_all_occurrence(context: AssetExecutionContext, duckdb: DuckDBResource, postgresql: PostgreSQLResource) -> dg.MaterializeResult:
    with duckdb.get_connection() as conn:
        result = conn.execute("""
            INSTALL spatial;
            LOAD spatial;

            SELECT
                taxon_key,
                ST_AsWKB(occurrence_coordinates) AS wkb
            FROM occurrences_by_taxon_key;
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
    deps=[vernacular_name_map]
)
def species_names(context: AssetExecutionContext, duckdb: DuckDBResource, postgresql: PostgreSQLResource) -> dg.MaterializeResult:
    with duckdb.get_connection() as conn:
        result = conn.execute("""
            SELECT
                taxon_key,
                vernacular_name_deu,
                vernacular_name_eng,
                vernacular_name_fra,
            FROM vernacular_name_map;
        """).fetch_arrow_table()

    data = [
        (int(row["taxon_key"]), row["vernacular_name_deu"], row["vernacular_name_eng"], row["vernacular_name_fra"])
        for row in result.to_pylist()
    ]

    with postgresql.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS species_names (
                    taxon_key INTEGER PRIMARY KEY,
                    vernacular_name_deu VARCHAR,
                    vernacular_name_eng VARCHAR,
                    vernacular_name_fra VARCHAR
                );
            """)

            cur.executemany("""
                INSERT INTO species_names (
                    taxon_key,
                    vernacular_name_deu,
                    vernacular_name_eng,
                    vernacular_name_fra
                )
                VALUES (
                    %s,
                    %s,
                    %s,
                    %s
                )
                ON CONFLICT (taxon_key) DO UPDATE
                SET
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
    code_version="0.1.0",
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
                coordinate_uncertainty_in_meters,
                ST_AsWKB(coordinates) AS wkb
            FROM geospatial_occurrences;
        """).fetch_arrow_table()

    data = [
        (row["gbif_id"],
         int(row["taxon_key"]),
         float(row["coordinate_uncertainty_in_meters"]) if row["coordinate_uncertainty_in_meters"] else None,
         psycopg.Binary(row["wkb"]))
        for row in result.to_pylist()
    ]

    with postgresql.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS occurrences (
                gbif_id VARCHAR PRIMARY KEY,
                taxon_key INTEGER,
                coordinate_uncertainty_in_meters DOUBLE PRECISION,
                coordinates geometry(Point, 4326)
            );
            """)

            cur.executemany("""
                INSERT INTO occurrences (
                    gbif_id,
                    taxon_key,
                    coordinate_uncertainty_in_meters,
                    coordinates
                )
                VALUES (
                    %s,
                    %s,
                    %s,
                    ST_GeomFromWKB(%s)
                )
                ON CONFLICT (gbif_id) DO UPDATE
                SET
                    taxon_key = EXCLUDED.taxon_key,
                    coordinate_uncertainty_in_meters = EXCLUDED.coordinate_uncertainty_in_meters,
                    coordinates = EXCLUDED.coordinates;
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
