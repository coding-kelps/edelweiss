from dagster_duckdb import DuckDBResource
import dagster as dg

from edelweiss.occurences import *

defs = dg.Definitions(
    assets=[
        generated_gbif_download_keys,
        raw_occurrences,
        pruned_occurrences,
        unique_taxon_keys,
        vernacular_name_map,
        living_species_occurrences,
        vernacular_name_mapped_occurences
    ],
    resources={"duckdb": DuckDBResource(database="data/edelweiss.duckdb")},
)
