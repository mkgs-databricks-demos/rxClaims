"""NCPDP Rule Extraction Pipeline - Spark Declarative Pipeline.

Stage 1: specification_rules_raw (table)
  - Reads specification_chunks_by_segment
  - Calls ai_query for each chunk to extract structured rules
  - Parses JSON response and explodes into individual rules

Stage 2: specification_rules (materialized view)
  - Deduplicates on natural key (SQL MD5)
  - Resolves cross-segment references (SQL CASE)
  - Normalizes column names and bronze keys (SQL regex)
  - Applies data quality expectations
  - NO Python UDFs — all SQL expressions to avoid worker serialization issues
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, ArrayType, IntegerType
)

import sys
sys.path.insert(0, "../utilities")
from utils import EXTRACTION_PROMPT

# Pipeline configuration
catalog_use = spark.conf.get("catalog_use")
schema_use = spark.conf.get("schema_use")
source_table = f"{catalog_use}.{schema_use}.specification_chunks_by_segment"

MODEL_ENDPOINT = "databricks-claude-sonnet-4"


# =============================================================================
# STAGE 1: specification_rules_raw (table)
# =============================================================================

@dp.table(
    name="specification_rules_raw",
    comment="Raw LLM-extracted rules from NCPDP specification chunks. Each row is one rule.",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableDeletionVectors": "true",
        "delta.enableRowTracking": "true",
        "quality": "bronze",
    },
)
def specification_rules_raw():
    """Extract structured rules from each specification chunk via ai_query."""

    # Read chunks that contain extractable rules
    chunks_df = (
        spark.read.table(source_table)
        .filter(
            (F.col("has_field_table") == True) | (F.col("has_segment_questions") == True)
        )
    )

    # Build prompt column using F.lit() + F.concat() to avoid SQL escaping.
    # Then call ai_query via F.expr() referencing the column by name.
    # NOTE: spark.sql(..., args=...) is NOT supported in SDP (monkey-patched).
    with_prompt = chunks_df.withColumn(
        "_prompt_text",
        F.concat(
            F.lit(EXTRACTION_PROMPT),
            F.lit("\n\nSpecification text:\n"),
            F.col("chunk_text")
        )
    )

    extraction_result = with_prompt.withColumn(
        "raw_response",
        F.expr(f"ai_query('{MODEL_ENDPOINT}', _prompt_text)")
    ).select(
        F.col("chunk_id").alias("source_chunk_id"),
        F.lit(MODEL_ENDPOINT).alias("extraction_model"),
        F.current_timestamp().alias("extracted_at"),
        F.col("raw_response"),
    )

    # Define schema for parsing the JSON response
    rule_schema = ArrayType(StructType([
        StructField("rule_level", StringType()),
        StructField("segment_code", StringType()),
        StructField("segment_name", StringType()),
        StructField("field_code", StringType()),
        StructField("field_name", StringType()),
        StructField("transaction_types", ArrayType(StringType())),
        StructField("rule_type", StringType()),
        StructField("payer_usage", StringType()),
        StructField("condition", StringType()),
        StructField("condition_segment", StringType()),
        StructField("rule_text", StringType()),
        StructField("column_name", StringType()),
        StructField("data_type", StringType()),
        StructField("allowed_values", ArrayType(StringType())),
        StructField("format_pattern", StringType()),
        StructField("max_occurrences", IntegerType()),
    ]))

    # Clean response (strip markdown fences) and parse JSON
    parsed = extraction_result.withColumn(
        "cleaned_response",
        F.regexp_replace(
            F.regexp_replace(F.col("raw_response"), r"^```json?\s*", ""),
            r"\s*```\s*$", ""
        )
    ).withColumn(
        "rules_array",
        F.from_json(F.col("cleaned_response"), rule_schema)
    )

    # Explode array into individual rules
    exploded = parsed.filter(
        F.col("rules_array").isNotNull()
    ).select(
        F.col("source_chunk_id"),
        F.col("extraction_model"),
        F.col("extracted_at"),
        F.explode(F.col("rules_array")).alias("rule")
    ).select(
        F.col("source_chunk_id"),
        F.col("extraction_model"),
        F.col("extracted_at"),
        F.col("rule.rule_level"),
        F.col("rule.segment_code"),
        F.col("rule.segment_name"),
        F.col("rule.field_code"),
        F.col("rule.field_name"),
        F.col("rule.transaction_types"),
        F.col("rule.rule_type"),
        F.col("rule.payer_usage"),
        F.col("rule.condition"),
        F.col("rule.condition_segment"),
        F.col("rule.rule_text"),
        F.col("rule.column_name"),
        F.col("rule.data_type"),
        F.col("rule.allowed_values"),
        F.col("rule.format_pattern"),
        F.col("rule.max_occurrences"),
    )

    return exploded


# =============================================================================
# STAGE 2: specification_rules (materialized view)
# Uses SQL expressions instead of Python UDFs to avoid worker serialization
# issues (workers can't import ../utilities/utils.py via sys.path).
# =============================================================================

# Field segment map as SQL CASE expression
_FIELD_SEGMENT_SQL = """
    CASE
        WHEN condition LIKE '%F_406_D6%' OR condition LIKE '%406-D6%' THEN '07'
        WHEN condition LIKE '%F_308_C8%' OR condition LIKE '%308-C8%' THEN '04'
        WHEN condition LIKE '%F_202_B2%' OR condition LIKE '%202-B2%' THEN 'HD'
        WHEN condition LIKE '%F_461_EU%' OR condition LIKE '%461-EU%' THEN '07'
        WHEN condition LIKE '%F_462_EV%' OR condition LIKE '%462-EV%' THEN '07'
        WHEN condition LIKE '%F_436_DN%' OR condition LIKE '%436-DN%' THEN '07'
        WHEN condition LIKE '%F_407_D7%' OR condition LIKE '%407-D7%' THEN '07'
        WHEN condition LIKE '%F_418_DI%' OR condition LIKE '%418-DI%' THEN '07'
        WHEN condition LIKE '%F_414_DE%' OR condition LIKE '%414-DE%' THEN '07'
        ELSE NULL
    END
"""


@dp.expect("rule_id_not_null", "rule_id IS NOT NULL")
@dp.expect("segment_code_not_null", "segment_code IS NOT NULL")
@dp.expect("rule_level_valid", "rule_level IN ('TRANSACTION', 'FIELD')")
@dp.expect("bronze_key_format", "bronze_key IS NULL OR (SUBSTRING(bronze_key, 1, 2) = 'F_' AND LENGTH(bronze_key) >= 7)")
@dp.materialized_view(
    name="specification_rules",
    comment="Production NCPDP validation rules - deduplicated, enriched, ready for silver-layer codegen and VS indexing.",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.enableDeletionVectors": "true",
        "delta.enableRowTracking": "true",
        "quality": "silver",
    },
)
def specification_rules():
    """Deduplicate, enrich, and validate extracted rules."""
    from pyspark.sql.window import Window

    raw_table = spark.read.table(f"{catalog_use}.{schema_use}.specification_rules_raw")
    chunks_table = spark.read.table(source_table).select(
        F.col("chunk_id"), F.col("segment_code").alias("_chunk_segment_code")
    )

    # Backfill null segment_code from source chunk metadata
    raw = raw_table.join(
        chunks_table,
        raw_table["source_chunk_id"] == chunks_table["chunk_id"],
        "left"
    ).withColumn(
        "segment_code",
        F.coalesce(F.col("segment_code"), F.col("_chunk_segment_code"))
    ).drop("chunk_id", "_chunk_segment_code")

    # Step 1: Generate rule_id via SQL MD5 (no UDF needed)
    with_id = raw.withColumn(
        "rule_id",
        F.expr("""
            SUBSTRING(
                MD5(CONCAT_WS('|',
                    COALESCE(segment_code, ''),
                    COALESCE(field_code, ''),
                    COALESCE(rule_type, ''),
                    COALESCE(condition, ''),
                    COALESCE(CAST(transaction_types AS STRING), '')
                )),
                1, 16
            )
        """)
    )

    # Step 2: Deduplicate on rule_id - keep row with longest rule_text
    window = Window.partitionBy("rule_id").orderBy(
        F.length(F.coalesce(F.col("rule_text"), F.lit(""))).desc(),
        F.size(F.coalesce(F.col("allowed_values"), F.array())).desc(),
        F.col("extracted_at").desc()
    )

    deduped = with_id.withColumn(
        "_rn", F.row_number().over(window)
    ).filter(F.col("_rn") == 1).drop("_rn")

    # Step 3: Enrich via SQL expressions (no Python UDFs)
    # bronze_key: extract field code components via regex -> F_NNN_XX format
    enriched = deduped.withColumn(
        "bronze_key",
        F.when(
            F.col("field_code").isNotNull(),
            F.expr("""
                CONCAT('F_',
                    LPAD(REGEXP_EXTRACT(field_code, '(\\d+)', 1), 3, '0'),
                    '_',
                    REGEXP_EXTRACT(field_code, '[- ](\\w+)$', 1)
                )
            """)
        )
    ).withColumn(
        # normalize column_name: lower, replace non-alnum with _, trim, max 63
        "column_name",
        F.when(
            F.col("field_name").isNotNull(),
            F.coalesce(
                F.col("column_name"),
                F.expr("""
                    SUBSTRING(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(
                                REGEXP_REPLACE(LOWER(field_name), '[^a-z0-9]+', '_'),
                                '^_+|_+$', ''
                            ),
                            '_+', '_'
                        ),
                        1, 63
                    )
                """)
            )
        ).otherwise(F.col("column_name"))
    ).withColumn(
        # resolve condition_segment via CASE WHEN map
        "condition_segment",
        F.coalesce(
            F.col("condition_segment"),
            F.expr(_FIELD_SEGMENT_SQL)
        )
    ).withColumn(
        # column_comment: "field_code | field_name | payer_usage" truncated to 255
        "column_comment",
        F.when(
            F.col("field_code").isNotNull(),
            F.expr("""
                SUBSTRING(
                    CONCAT(
                        COALESCE(field_code, ''),
                        ' | ',
                        COALESCE(field_name, ''),
                        CASE WHEN payer_usage IS NOT NULL
                             THEN CONCAT(' | ', payer_usage)
                             ELSE '' END
                    ),
                    1, 255
                )
            """)
        )
    )

    # Step 4: Select final columns in schema order
    result = enriched.select(
        "rule_id",
        "source_chunk_id",
        "rule_level",
        "rule_type",
        "segment_code",
        "segment_name",
        "field_code",
        "field_name",
        "bronze_key",
        "transaction_types",
        "payer_usage",
        "condition",
        "condition_segment",
        "rule_text",
        "allowed_values",
        "format_pattern",
        "max_occurrences",
        "column_name",
        "data_type",
        "column_comment",
        "extraction_model",
        "extracted_at",
        F.lit(None).cast("double").alias("confidence"),
    )

    return result
