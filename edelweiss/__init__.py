from dagster_duckdb import DuckDBResource
import dagster as dg
from dagster import EnvVar

from edelweiss.assets.occurences import *
from edelweiss.resources.gbif import GBIFAPIResource

defs = dg.Definitions(
    assets=[
        generated_gbif_download_keys,
        raw_occurrences,
        pruned_occurrences,
        unique_taxon_keys,
        vernacular_name_map,
        vernacular_name_mapped_occurrences
    ],
    resources={
        "duckdb": DuckDBResource(database="data/edelweiss.duckdb"),
        "gbif": GBIFAPIResource(
            username=EnvVar("GBIF_USER"),
            password=EnvVar("GBIF_PWD"),
            email=EnvVar("GBIF_EMAIL")
        ),
    },
)
