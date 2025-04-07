from dagster_duckdb import DuckDBResource
import dagster as dg

from edelweiss.species import species

defs = dg.Definitions(
    assets=[
        species,
    ],
    resources={"duckdb": DuckDBResource(database="data/mydb.duckdb")},
)
