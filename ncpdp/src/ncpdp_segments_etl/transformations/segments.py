"""
ncpdp_segments_etl — Silver Layer Segment Transformations

Metadata-driven pipeline that reads specification_rules at definition time
and dynamically generates one materialized view per NCPDP segment.

Each silver table:
  1. Filters bronze_requests by segment (WHERE request_segment = 'S_XX')
  2. Pivots key-value rows into typed columns using rules metadata
  3. Casts values to proper SQL types (STRING, INT, DECIMAL, DATE)
  4. Applies @dp.expect quality rules (MANDATORY, ALLOWED_VALUES, FORMAT)
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from utilities.utils import SegmentBuilder, SegmentDefinition, SILVER_TABLE_PROPERTIES
from typing import Dict, List

# ─── Configuration from pipeline settings ─────────────────────────────────────
catalog = spark.conf.get("catalog_use")
schema = spark.conf.get("schema_use")
bronze_requests_table = f"{catalog}.{schema}.claimbilling_bronze_requests"

# ─── Build segment definitions from specification_rules ─────────────────────
builder = SegmentBuilder(spark, catalog, schema)
segments = builder.build_all_segments()


# ─── Factory function for segment table registration ───────────────────────
def register_segment_table(
    table_fqn: str,
    pivot_sql: str,
    expectations: Dict[str, str],
    schema_ddl: str,
):
    """
    Register a single silver table with the SDP pipeline.

    Called once per segment inside the loop. The function call creates a new
    scope, so the inner closure correctly captures pivot_sql by value.

    The schema_ddl parameter embeds column COMMENT clauses directly in the
    table definition — the supported way to apply column comments in SDP.
    """
    if expectations:
        @dp.table(
            name=table_fqn,
            schema=schema_ddl,
            table_properties=SILVER_TABLE_PROPERTIES,
            cluster_by_auto=True,
        )
        @dp.expect_all(expectations)
        def _segment_table():
            return spark.sql(pivot_sql)
    else:
        @dp.table(
            name=table_fqn,
            schema=schema_ddl,
            table_properties=SILVER_TABLE_PROPERTIES,
            cluster_by_auto=True,
        )
        def _segment_table():
            return spark.sql(pivot_sql)


# ─── Register all segment tables ───────────────────────────────────────────
for _seg in segments:
    register_segment_table(
        table_fqn=f"{catalog}.{schema}.{_seg.table_name}",
        pivot_sql=builder.build_pivot_sql(_seg),
        expectations=builder.build_expectations(_seg),
        schema_ddl=builder.build_schema_ddl(_seg),
    )

