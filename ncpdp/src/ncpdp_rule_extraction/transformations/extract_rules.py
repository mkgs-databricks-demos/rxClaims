"""NCPDP Rule Extraction Pipeline — Spark Declarative Pipeline.

Stage 1: specification_rules_raw (streaming table)
  - Reads specification_chunks_by_segment
  - Calls ai_query for each chunk to extract structured rules
  - Parses JSON response and explodes into individual rules

Stage 2: specification_rules (materialized view)
  - Deduplicates on natural key
  - Resolves cross-segment references
  - Normalizes column names and bronze keys
  - Applies data quality expectations
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, ArrayType, IntegerType
)

import sys
sys.path.insert(0, "../utilities")
from utils import (
    EXTRACTION_PROMPT,
    generate_rule_id,
    field_code_to_bronze_key,
    normalize_column_name,
    generate_column_comment,
    resolve_condition_segment,
)

# Pipeline configuration
catalog_use = spark.conf.get("catalog_use")
schema_use = spark.conf.get("schema_use")
source_table = f"{catalog_use}.{schema_use}.specification_chunks_by_segment"

MODEL_ENDPOINT = "databricks-claude-sonnet-4"


# =============================================================================
# STAGE 1: specification_rules_raw (streaming table)
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

    # Add prompt column — concatenate system prompt with chunk text
    prompt_col = F.concat(
        F.lit(EXTRACTION_PROMPT),
        F.lit("\n\nSpecification text:\n"),
        F.col("chunk_text")
    )

    # Call ai_query via SQL expression — register chunks as temp view
    chunks_df.createOrReplaceTempView("_extraction_chunks")

    # Use parameterized SQL for the ai_query call
    extraction_result = spark.sql(
        """
        SELECT
            chunk_id AS source_chunk_id,
            segment_code,
            transaction_type,
            :model_endpoint AS extraction_model,
            CURRENT_TIMESTAMP() AS extracted_at,
            ai_query(
                :model_endpoint,
                CONCAT(:prompt, '\n\nSpecification text:\n', chunk_text)
            ) AS raw_response
        FROM _extraction_chunks
        """,
        args={
            "model_endpoint": MODEL_ENDPOINT,
            "prompt": EXTRACTION_PROMPT,
        }
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
# =============================================================================

# Register UDFs for post-processing
generate_rule_id_udf = F.udf(generate_rule_id, StringType())
field_code_to_bronze_key_udf = F.udf(field_code_to_bronze_key, StringType())
normalize_column_name_udf = F.udf(normalize_column_name, StringType())
generate_column_comment_udf = F.udf(generate_column_comment, StringType())
resolve_condition_segment_udf = F.udf(resolve_condition_segment, StringType())


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

    raw = spark.read.table("specification_rules_raw")

    # Step 1: Generate rule_id for deduplication
    with_id = raw.withColumn(
        "rule_id",
        generate_rule_id_udf(
            F.col("segment_code"),
            F.col("field_code"),
            F.col("rule_type"),
            F.col("condition"),
            F.col("transaction_types")
        )
    )

    # Step 2: Deduplicate on rule_id - keep row with longest rule_text
    from pyspark.sql.window import Window

    window = Window.partitionBy("rule_id").orderBy(
        F.length(F.coalesce(F.col("rule_text"), F.lit(""))).desc(),
        F.size(F.coalesce(F.col("allowed_values"), F.array())).desc(),
        F.col("extracted_at").desc()
    )

    deduped = with_id.withColumn(
        "_rn", F.row_number().over(window)
    ).filter(F.col("_rn") == 1).drop("_rn")

    # Step 3: Enrich - bronze_key, column_name normalization, cross-segment resolution
    enriched = deduped.withColumn(
        "bronze_key",
        F.when(
            F.col("field_code").isNotNull(),
            field_code_to_bronze_key_udf(F.col("field_code"))
        )
    ).withColumn(
        "column_name",
        F.when(
            F.col("field_name").isNotNull(),
            F.coalesce(
                F.col("column_name"),
                normalize_column_name_udf(F.col("field_name"))
            )
        ).otherwise(F.col("column_name"))
    ).withColumn(
        "condition_segment",
        F.coalesce(
            F.col("condition_segment"),
            resolve_condition_segment_udf(F.col("condition"), F.col("segment_code"))
        )
    ).withColumn(
        "column_comment",
        F.when(
            F.col("field_code").isNotNull(),
            generate_column_comment_udf(
                F.col("field_code"), F.col("field_name"), F.col("payer_usage")
            )
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
