from dagster_duckdb import DuckDBResource
import dagster as dg

from edelweiss.species import *

defs = dg.Definitions(
    assets=[
        raw_occurences,
        pruned_occurences,
        unique_taxon_keys,
        vernacular_name_map,
        living_species_occurences,
        vernacular_name_mapped_occurences
    ],
    resources={"duckdb": DuckDBResource(database="data/edelweiss.duckdb")},
)
