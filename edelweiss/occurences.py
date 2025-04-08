from dagster_duckdb import DuckDBResource
import dagster as dg
from pygbif import occurrences, species
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

ANIMALIA_KINGDOM_KEY="1"
FRENCH_ALPS_APPROXIMATE_GEOMETRY="POLYGON((4.63127 44.84424,7.5505 44.84424,7.5505 46.81983,4.63127 46.81983,4.63127 44.84424))"
GBIF_API_LIMIT=300

@dg.asset(
    compute_kind="duckdb",
    group_name="ingestion",
    code_version="0.2.0",
    description="Extract raw observation occurences of animal species in the French Alps from the GBIF API",
    tags = {"gbif": ""}
)
def raw_occurrences(duckdb: DuckDBResource) -> dg.MaterializeResult:
    with duckdb.get_connection() as conn:
        res = occurrences.search(
            kingdomKey=ANIMALIA_KINGDOM_KEY,
            year="2025", # Can be changed for a range (e.g.: 2024,2025)
            continent="europe",
            country="FR",
            geometry=FRENCH_ALPS_APPROXIMATE_GEOMETRY,
            limit=GBIF_API_LIMIT # Actual Max of the API
        )

        df = pd.DataFrame(res["results"])

        conn.register("df_view", df)
        conn.execute("CREATE TABLE IF NOT EXISTS raw_occurrences AS SELECT * FROM df_view")

        row_count = conn.execute("select count(*) from raw_occurrences").fetchone()
        count = row_count[0] if row_count else 0

        return dg.MaterializeResult(
            metadata={
                "row_count": dg.MetadataValue.int(count),
            }
        )
    
@dg.asset(
    compute_kind="duckdb",
    group_name="ingestion",
    code_version="0.2.0",
    description="Create a new table \"pruned_occurrences\" with only revelant columns for the edelweiss preprocessing pipeline from \"raw_occurrences\"",
    deps=[raw_occurrences]
)
def pruned_occurrences(duckdb: DuckDBResource) -> dg.MaterializeResult:
    with duckdb.get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS pruned_occurrences AS
            SELECT 
                taxonKey, 
                scientificName, 
                coordinateUncertaintyInMeters, 
                decimalLatitude, 
                decimalLongitude, 
                vitality
            FROM raw_occurrences;
        """)
        row_count = conn.execute("select count(*) from pruned_occurrences").fetchone()
        count = row_count[0] if row_count else 0

        return dg.MaterializeResult(
            metadata={
                "row_count": dg.MetadataValue.int(count),
            }
        )

@dg.asset(
    compute_kind="duckdb",
    group_name="ingestion",
    code_version="0.2.0",
    description="Create a new table \"unique_taxon_keys\" listing all unique GBIF taxon key from \"raw_occurrences\"",
    deps=[raw_occurrences]
)
def unique_taxon_keys(duckdb: DuckDBResource) -> dg.MaterializeResult:
    with duckdb.get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS unique_taxon_keys AS
            SELECT DISTINCT taxonKey FROM raw_occurrences;
        """)
        row_count = conn.execute("select count(*) from unique_taxon_keys").fetchone()
        count = row_count[0] if row_count else 0
        
        return dg.MaterializeResult(
            metadata={
                "row_count": dg.MetadataValue.int(count),
            }
        )

THREADPOOL_MAX_WORKER=10

def get_vernacular_name(taxonKey):
    try:
        res = species.name_usage(key=taxonKey)
        return res.get("vernacularName", None)
    except Exception as e:
        return None

@dg.asset(
    compute_kind="duckdb",
    group_name="ingestion",
    code_version="0.1.0",
    description="Create a new table \"vernacular_name_map\" mapping all vernacular name for all taxon key of \"unique_taxon_keys\"",
    tags = {"gbif": ""},
    deps=[unique_taxon_keys]
)
def vernacular_name_map(duckdb: DuckDBResource) -> dg.MaterializeResult:
    with duckdb.get_connection() as conn:
        df = conn.sql("SELECT * FROM unique_taxon_keys;").df()

        unique_keys = df["taxonKey"].unique()
        vernacular_name_map = {}

        with ThreadPoolExecutor(max_workers=THREADPOOL_MAX_WORKER) as executor:
            future_to_key = {executor.submit(get_vernacular_name, key): key for key in unique_keys}
            for future in as_completed(future_to_key):
                key = future_to_key[future]
                try:
                    vernacular_name_map[key] = future.result()
                except Exception:
                    vernacular_name_map[key] = None

        vernacular_name_map_df = pd.DataFrame(vernacular_name_map.items(), columns=["taxonKey", "vernacularName"])

        conn.register("vernacular_name_map_df_view", vernacular_name_map_df)
        conn.execute("CREATE TABLE IF NOT EXISTS vernacular_name_map AS SELECT * FROM vernacular_name_map_df_view")

        row_count = conn.execute("select count(*) from vernacular_name_map").fetchone()
        count = row_count[0] if row_count else 0

        return dg.MaterializeResult(
            metadata={
                "row_count": dg.MetadataValue.int(count),
            }
        )

@dg.asset(
    compute_kind="duckdb",
    group_name="ingestion",
    code_version="0.2.0",
    description="Create a new table \"living_species_occurences\" removing all row that describe a dead species observation occurences from \"pruned_occurrences\"",
    deps=[pruned_occurrences]
)
def living_species_occurrences(duckdb: DuckDBResource) -> dg.MaterializeResult:
    with duckdb.get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS living_species_occurences AS
            SELECT
                taxonKey, 
                scientificName, 
                coordinateUncertaintyInMeters, 
                decimalLatitude, 
                decimalLongitude, 
            FROM pruned_occurrences
            WHERE vitality IS DISTINCT FROM 'dead';
        """)
        row_count = conn.execute("select count(*) from living_species_occurences").fetchone()
        count = row_count[0] if row_count else 0

        return dg.MaterializeResult(
            metadata={
                "row_count": dg.MetadataValue.int(count),
            }
        )

@dg.asset(
    compute_kind="duckdb",
    group_name="ingestion",
    code_version="0.2.0",
    description="Create a new table \"vernacular_name_mapped_occurences\" mapping all vernacular name for each row of \"living_species_occurences\" from \"vernacular_name_map\"",
    deps=[vernacular_name_map, living_species_occurrences]
)
def vernacular_name_mapped_occurences(duckdb: DuckDBResource) -> dg.MaterializeResult:
    with duckdb.get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS vernacular_name_mapped_occurences AS
            SELECT 
                l.*, 
                v.vernacularName
            FROM living_species_occurences l
            LEFT JOIN vernacular_name_map v
            ON l.taxonKey = v.taxonKey;
        """)
        row_count = conn.execute("select count(*) from vernacular_name_mapped_occurences").fetchone()
        count = row_count[0] if row_count else 0

        return dg.MaterializeResult(
            metadata={
                "row_count": dg.MetadataValue.int(count),
            }
        )
