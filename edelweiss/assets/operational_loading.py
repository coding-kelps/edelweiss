import psycopg
import dagster as dg
from dagster_duckdb import DuckDBResource
from dagster import AssetExecutionContext
from edelweiss.resources import PostgreSQLResource
from edelweiss.constants import OUTPUT_DIR
import pandas as pd
from edelweiss.assets.ingestion import (
    geospatial_occurrences,
    vernacular_names,
    scientific_names
)

@dg.asset(
    kinds={"python", "duckdb", "postgresql"},
    group_name="operational_loading",
    code_version="0.1.0",
    description="",
    deps=[geospatial_occurrences]
)
def species_all_occurrence(context: AssetExecutionContext, duckdb: DuckDBResource, postgresql: PostgreSQLResource) -> dg.MaterializeResult:
    with duckdb.get_connection() as conn:
        result = conn.execute(f"""
            INSTALL spatial;
            LOAD spatial;
                              
            SELECT
                taxon_key,
                ST_AsWKB(ST_Collect(list(coordinates))) AS wkb
            FROM read_parquet('{OUTPUT_DIR}/geospatial-occurrences.parquet')
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
    group_name="operational_loading",
    code_version="0.1.0",
    description="",
    deps=[vernacular_names, scientific_names]
)
def species_names(context: AssetExecutionContext, duckdb: DuckDBResource, postgresql: PostgreSQLResource) -> dg.MaterializeResult:
    with duckdb.get_connection() as conn:
        result = conn.execute(f"""
            SELECT
                scientific_names.taxon_key,
                scientific_names.scientific_name,
                vernacular_names.vernacular_name_deu,
                vernacular_names.vernacular_name_eng,
                vernacular_names.vernacular_name_fra
            FROM read_parquet('{OUTPUT_DIR}/scientific-names.parquet') AS 'scientific_names'
            JOIN read_parquet('{OUTPUT_DIR}/vernacular-names.parquet') AS 'vernacular_names' USING (taxon_key);
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
    group_name="operational_loading",
    code_version="0.2.0",
    description="",
    deps=[geospatial_occurrences]
)
def occurrences(context: AssetExecutionContext, duckdb: DuckDBResource, postgresql: PostgreSQLResource) -> dg.MaterializeResult:
    with duckdb.get_connection() as conn:
        result = conn.execute(f"""
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
            FROM read_parquet('{OUTPUT_DIR}/geospatial-occurrences.parquet');
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
