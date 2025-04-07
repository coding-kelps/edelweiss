from dagster_duckdb import DuckDBResource
import dagster as dg
from pygbif import occurrences, species
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

ANIMALIA_KINGDOM_KEY="1"
FRENCH_ALPS_APPROXIMATE_GEOMETRY="POLYGON((4.63127 44.84424,7.5505 44.84424,7.5505 46.81983,4.63127 46.81983,4.63127 44.84424))"
THREADPOOL_MAX_WORKER=10

def get_vernacular_name(taxonKey):
    try:
        res = species.name_usage(key=taxonKey)
        return res.get("vernacularName", None)
    except:
        return None

@dg.asset(
    compute_kind="duckdb",
    group_name="ingestion",
)
def species(duckdb: DuckDBResource) -> dg.MaterializeResult:
    with duckdb.get_connection() as conn:
        res = occurrences.search(
            kingdomKey=ANIMALIA_KINGDOM_KEY,
            year="2025", # Can be changed for a range (e.g.: 2024,2025)
            continent="europe",
            country="FR",
            geometry=FRENCH_ALPS_APPROXIMATE_GEOMETRY,
            limit=300 # Actual Max of the API
        )

        df = pd.DataFrame(res["results"])

        preprocessed_df = df[["taxonKey", "scientificName", "coordinateUncertaintyInMeters", "decimalLatitude", "decimalLongitude", "vitality"]]

        # Remove dead species
        preprocessed_df = preprocessed_df[preprocessed_df["vitality"] != "dead"]

        # Drop vitality as it is irrelevent after dead species were removed
        preprocessed_df = preprocessed_df.drop(columns="vitality")

        unique_keys = preprocessed_df["taxonKey"].unique()
        vernacular_name_map = {}

        with ThreadPoolExecutor(max_workers=THREADPOOL_MAX_WORKER) as executor:
            future_to_key = {executor.submit(get_vernacular_name, key): key for key in unique_keys}
            for future in as_completed(future_to_key):
                key = future_to_key[future]
                try:
                    vernacular_name_map[key] = future.result()
                except Exception:
                    vernacular_name_map[key] = None

        preprocessed_df["vernacularName"] = preprocessed_df["taxonKey"].map(vernacular_name_map)

        conn.register("df_view", preprocessed_df)
        conn.execute("CREATE TABLE IF NOT EXISTS species_data AS SELECT * FROM df_view")

        return dg.MaterializeResult(
            metadata={}
        )
