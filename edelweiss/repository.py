from dagster_duckdb import DuckDBResource
import dagster as dg
from dagster import EnvVar

from edelweiss.assets.occurrences import *
from edelweiss.resources.gbif import GBIFAPIResource
from edelweiss.resources.postgresql import PostgreSQLResource

from edelweiss.constants import OUTPUT_DIR

os.makedirs(OUTPUT_DIR, exist_ok=True)

gbif_archive_dir = f"{OUTPUT_DIR}/gbif-archives"
os.makedirs(gbif_archive_dir, exist_ok=True)

defs = dg.Definitions(
    assets=[
        partitioned_raw_occurrences,
        raw_occurrences,
        pruned_occurrences,
        geospatial_occurrences,
        unique_taxon_keys,
        vernacular_names,
        scientific_names,
        species_all_occurrence,
        species_names,
        occurrences,
    ],
    resources={
        "duckdb": DuckDBResource(database=":memory:"),
        "gbif": GBIFAPIResource(
            username=EnvVar("GBIF_USER"),
            password=EnvVar("GBIF_PWD"),
            email=EnvVar("GBIF_EMAIL"),
            dir=gbif_archive_dir,
        ),
        "postgresql": PostgreSQLResource(
            username=EnvVar("POSTGRES_USER"),
            password=EnvVar("POSTGRES_PASSWORD"),
            host=EnvVar("POSTGRES_HOST"),
            port=EnvVar("POSTGRES_PORT"),
            db_name=EnvVar("POSTGRES_DB"),
        ),
    },
)
