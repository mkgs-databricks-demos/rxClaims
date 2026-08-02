"""
ncpdp_segments_etl — Apply Column Comments

Reads specification_rules where column_comment IS NOT NULL and generates
ALTER TABLE ... ALTER COLUMN ... COMMENT DDL statements to apply
NCPDP field documentation to silver table columns.

This runs as a post-processing step after all silver tables are created.
Implemented as a materialized view that produces the DDL audit log.
"""
from pyspark import pipelines as dp
from utilities.utils import SegmentBuilder, SEGMENT_CONFIG, SILVER_TABLE_PROPERTIES

catalog = spark.conf.get("catalog_use")
schema = spark.conf.get("schema_use")

# ─── Build and execute column comment DDL ──────────────────────────────────

builder = SegmentBuilder(spark, catalog, schema)
segments = builder.build_all_segments()

# Collect all DDL statements
ddl_records = []

for seg in segments:
    table_fqn = f"{catalog}.{schema}.{seg.table_name}"
    for field in seg.fields:
        if field.column_comment:
            # Escape single quotes in comments
            safe_comment = field.column_comment.replace("'", "\\'")
            ddl = (
                f"ALTER TABLE {table_fqn} "
                f"ALTER COLUMN `{field.column_name}` "
                f"COMMENT '{safe_comment}'"
            )
            ddl_records.append({
                "table_name": table_fqn,
                "column_name": field.column_name,
                "comment": field.column_comment,
                "ddl_statement": ddl,
            })

# Execute DDL statements
for rec in ddl_records:
    try:
        spark.sql(rec["ddl_statement"])
        rec["status"] = "SUCCESS"
    except Exception as e:
        rec["status"] = f"FAILED: {str(e)[:200]}"


# ─── Audit table: record what comments were applied ─────────────────────

@dp.table(
    name=f"{catalog}.{schema}.claimbilling_silver_column_comments_log",
    table_properties=SILVER_TABLE_PROPERTIES,
)
def column_comments_log():
    """Audit log of column comment DDL applications."""
    from pyspark.sql import Row
    from pyspark.sql.functions import current_timestamp

    if ddl_records:
        df = spark.createDataFrame(
            [Row(**rec) for rec in ddl_records]
        ).withColumn("applied_at", current_timestamp())
        return df
    else:
        # Return empty DataFrame with schema
        from pyspark.sql.types import StructType, StructField, StringType, TimestampType
        empty_schema = StructType([
            StructField("table_name", StringType(), True),
            StructField("column_name", StringType(), True),
            StructField("comment", StringType(), True),
            StructField("ddl_statement", StringType(), True),
            StructField("status", StringType(), True),
            StructField("applied_at", TimestampType(), True),
        ])
        return spark.createDataFrame([], empty_schema)
