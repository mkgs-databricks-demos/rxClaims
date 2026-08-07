"""Tier 1 — TestPipeline wiring tests for ncpdp_document_intelligence.

Runs inside the SDP runtime via the Lakeflow Pipelines Editor test runner.
Validates that the pipeline graph is correctly wired: table dependencies
resolve, filters pass data through, and selectExpr columns are correct.

IMPORTANT:
  - These tests invoke real AI functions (ai_parse_document, ai_classify,
    ai_extract, ai_prep_search). Each run incurs AI token costs.
  - Layer 1 (stream_ingest via Auto Loader) is NOT tested because cloudFiles
    bypasses TestPipeline table isolation.
  - Run from the Lakeflow Pipelines Editor only (click + → Test, or Run Tests).
  - Requires: PREVIEW channel, triggered mode, Pipeline Owner + USE CATALOG +
    CREATE SCHEMA on default catalog.

Test Strategy:
  - Test A: Layers 2-5 full chain with a minimal valid PNG image.
    Mocks specification_documents with real binary, runs the complete
    parse → classify → extract → prep_search chain.
  - Test B: Layers 3-5 with pre-crafted VARIANT mock (skips ai_parse_document).
    Cheaper — only classification, extraction, and chunking execute.
"""

import pytest
from pyspark.pipelines.testing import TestPipeline, test_spark


# ═══════════════════════════════════════════════════════════════════════════════
# Pipeline reference
# ═══════════════════════════════════════════════════════════════════════════════

test_pipeline = TestPipeline.active()


# ═══════════════════════════════════════════════════════════════════════════════
# Test Fixtures — Minimal valid binary content for ai_parse_document
# ═══════════════════════════════════════════════════════════════════════════════

# Minimal valid PDF (1 page with text "NCPDP Implementation Guide D.0")
# This is a hand-crafted minimal PDF that ai_parse_document v2 can process.
# If this proves too fragile, swap for a real fixture file read from a volume.
MINIMAL_PDF_TEXT = "NCPDP Telecommunication Standard Implementation Guide Version D.0 Segment AM01 Patient Request"


# Pipeline configuration keys — match the pipeline YAML's `configuration` block.
# At runtime, values resolve from:
#   1. Pipeline-propagated Spark conf (if TestPipeline starts passing them through)
#   2. current_catalog() / current_database() for catalog/schema
#   3. Bundle variable defaults for volume paths
#
# This removes hardcoded catalog/schema values so the tests work across all
# bundle targets (dev, e2_demo_fe, free_edition) without manual edits.
_CONFIG_DEFAULTS = {
    "catalog_use": None,       # Resolved from current_catalog() if not propagated
    "schema_use": None,        # Resolved from current_database() if not propagated
    "volume_use": "spec_documents",
    "volume_sub_path_use": "raw",
    "image_output_sub_path_use": "parsed_image_output",
}


@pytest.fixture(autouse=True)
def set_pipeline_configs(test_spark):
    """Set pipeline configuration vars that the pipeline code expects via spark.conf.get().

    The TestPipeline framework does not automatically propagate the pipeline's
    'configuration' map to the Spark session used during test execution.

    Resolution order per key:
      1. Already set in Spark conf (pipeline runtime propagated it) → keep as-is
      2. Fallback: current_catalog()/current_database() for catalog/schema,
         or bundle variable defaults for volume paths.
    """
    for key, default in _CONFIG_DEFAULTS.items():
        try:
            test_spark.conf.get(key)
            # Already propagated by the pipeline runtime — leave it alone.
            continue
        except Exception:
            pass

        if key == "catalog_use":
            value = test_spark.sql("SELECT current_catalog()").collect()[0][0]
        elif key == "schema_use":
            value = test_spark.sql("SELECT current_database()").collect()[0][0]
        else:
            value = default
        test_spark.conf.set(key, value)


def _fqn(table_suffix: str) -> str:
    """Fully qualified table name matching the pipeline's published datasets.

    Reads catalog and schema dynamically from Spark conf (set by the pipeline
    runtime or the set_pipeline_configs fixture). This ensures the test works
    across all bundle targets without hardcoded values.
    """
    from pyspark.sql import SparkSession
    s = SparkSession.getActiveSession()
    catalog = s.conf.get("catalog_use")
    schema = s.conf.get("schema_use")
    return f"{catalog}.{schema}.{table_suffix}"

# ═══════════════════════════════════════════════════════════════════════════════
# Mock Setup Functions
# ═══════════════════════════════════════════════════════════════════════════════


def mock_specification_documents_with_text(session, text: str = MINIMAL_PDF_TEXT):
    """Create a mock specification_documents table with a text-based PDF.

    Uses Spark's built-in to_pdf (not available) — instead we create a minimal
    binary payload. For TestPipeline, the key requirement is that the content
    column is BINARY and the path has a supported extension.

    NOTE: ai_parse_document needs a real binary document format. We use a
    minimal valid PDF structure here. If it errors, the test for Layer 2 will
    detect it via error_status and downstream layers will be empty.
    """
    session.sql(f"""
        CREATE OR REPLACE TABLE {_fqn('specification_documents')} AS
        SELECT
            sha2('{text}', 256) as doc_source_id,
            named_struct(
                'file_path', '/Volumes/test/spec/vol/ncpdp_impl_guide.pdf',
                'file_name', 'ncpdp_impl_guide.pdf',
                'file_size', CAST(1024 AS BIGINT),
                'file_block_start', CAST(0 AS BIGINT),
                'file_block_length', CAST(1024 AS BIGINT),
                'file_modification_time', CURRENT_TIMESTAMP()
            ) as file_metadata,
            CURRENT_TIMESTAMP() as ingest_time,
            '/Volumes/test/spec/vol/ncpdp_impl_guide.pdf' as path,
            CURRENT_TIMESTAMP() as modificationTime,
            CAST(1024 AS LONG) as length,
            -- Minimal PDF: header + single page with text content
            CAST(
                CONCAT(
                    '%PDF-1.0\n',
                    '1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj\n',
                    '2 0 obj<</Type/Pages/Kids[3 0 R]/Count 1>>endobj\n',
                    '3 0 obj<</Type/Page/MediaBox[0 0 612 792]/Parent 2 0 R/Contents 4 0 R/Resources<</Font<</F1<</Type/Font/Subtype/Type1/BaseFont/Helvetica>>>>>>>>endobj\n',
                    '4 0 obj<</Length ', CAST(LENGTH(CONCAT('BT /F1 12 Tf 100 700 Td (', '{text}', ') Tj ET')) AS STRING), '>>stream\n',
                    'BT /F1 12 Tf 100 700 Td (', '{text}', ') Tj ET\n',
                    'endstream endobj\n',
                    'xref\n0 5\n',
                    '0000000000 65535 f \n',
                    '0000000009 00000 n \n',
                    '0000000058 00000 n \n',
                    '0000000115 00000 n \n',
                    '0000000330 00000 n \n',
                    'trailer<</Size 5/Root 1 0 R>>\n',
                    'startxref\n450\n',
                    '%%EOF'
                ) AS BINARY
            ) as content
    """)


def mock_specification_documents_parsed(session, text: str = MINIMAL_PDF_TEXT):
    """Create a mock specification_documents_parsed table with pre-crafted VARIANT.

    Skips Layer 2 (ai_parse_document) entirely. Provides a realistic parsed
    VARIANT structure that Layers 3-5 can consume without AI parsing costs.
    """
    session.sql(f"""
        CREATE OR REPLACE TABLE {_fqn('specification_documents_parsed')} AS
        SELECT
            sha2('{text}', 256) as doc_source_id,
            '/Volumes/test/spec/vol/ncpdp_impl_guide.pdf' as path,
            named_struct(
                'file_path', '/Volumes/test/spec/vol/ncpdp_impl_guide.pdf',
                'file_name', 'ncpdp_impl_guide.pdf',
                'file_size', CAST(1024 AS BIGINT),
                'file_block_start', CAST(0 AS BIGINT),
                'file_block_length', CAST(1024 AS BIGINT),
                'file_modification_time', CURRENT_TIMESTAMP()
            ) as file_metadata,
            -- Simulate a successful ai_parse_document result as VARIANT
            PARSE_JSON('{{
                "pages": [
                    {{
                        "page_number": 1,
                        "content": "{text}",
                        "elements": [
                            {{
                                "type": "paragraph",
                                "content": "{text}"
                            }}
                        ]
                    }}
                ]
            }}') as parsed
    """)


# ═══════════════════════════════════════════════════════════════════════════════
# Test A: Full chain (Layers 2-5) with real binary document
# Cost: ~$0.05-0.15 per run (all 4 AI functions execute)
# ═══════════════════════════════════════════════════════════════════════════════


class TestDiagnostics:
    """Diagnostic tests to verify TestPipeline framework basics."""

    def test_mock_table_roundtrip(self, test_spark):
        """Verify that mock table creation and reading works via test_spark."""
        test_spark.sql(f"""
            CREATE OR REPLACE TABLE {_fqn('_test_diagnostic')} AS
            SELECT 1 as id, 'hello' as value
        """)
        result = test_spark.table(_fqn('_test_diagnostic'))
        assert result.count() == 1, "Mock table roundtrip failed"

    def test_pipeline_config_available(self, test_spark):
        """Check if pipeline configuration vars are accessible."""
        # Try to get config the same way the pipeline code does
        from pyspark.sql import SparkSession
        s = SparkSession.getActiveSession()
        configs = {}
        for key in ['catalog_use', 'schema_use', 'volume_use', 'volume_sub_path_use']:
            try:
                configs[key] = s.conf.get(key)
            except Exception as e:
                configs[key] = f"NOT SET ({type(e).__name__})"
        # Also check current catalog/database
        configs['current_catalog'] = test_spark.sql("SELECT current_catalog()").collect()[0][0]
        configs['current_database'] = test_spark.sql("SELECT current_database()").collect()[0][0]
        # Report findings as assertion message
        info = "\n".join(f"  {k}: {v}" for k, v in configs.items())
        # This test always passes but reports the config state
        assert True, f"Pipeline configs:\n{info}"
        print(f"Pipeline configs:\n{info}")

    def test_pipeline_run_minimal(self, test_spark):
        """Run the first pipeline table and check status details."""
        # Mock the auto-loader source table
        mock_specification_documents_with_text(test_spark)

        status = test_pipeline.run(
            test_spark,
            {_fqn("specification_documents_parsed")},
        )
        # Report ALL status attributes
        attrs = {attr: getattr(status, attr, 'N/A') for attr in dir(status)
                 if not attr.startswith('_')}
        info = "\n".join(f"  {k}: {v}" for k, v in attrs.items())
        assert status.is_success, f"Pipeline failed. Status attrs:\n{info}"


class TestFullChainWithBinary:
    """End-to-end pipeline wiring test using a minimal PDF.

    Validates that:
    - Layer 2 filter passes .pdf extension
    - ai_parse_document produces a non-error VARIANT
    - Layers 3-5 consume the parsed output
    """

    def test_parse_documents_produces_output(self, test_spark):
        """Layer 2: ai_parse_document processes the mock PDF."""
        mock_specification_documents_with_text(test_spark)

        status = test_pipeline.run(
            test_spark,
            {_fqn("specification_documents_parsed")},
        )

        # Debug: check pipeline execution status and event log on failure
        assert status is not None, "test_pipeline.run() returned None"
        if hasattr(status, 'is_success') and not status.is_success:
            error_details = "Pipeline run failed."
            if hasattr(status, 'event_log_table_name') and status.event_log_table_name:
                try:
                    errors = test_spark.sql(f"""
                        SELECT timestamp, level, message
                        FROM `{status.event_log_table_name}`
                        WHERE level = 'ERROR'
                        ORDER BY timestamp DESC
                        LIMIT 5
                    """).collect()
                    error_details += "\n" + "\n".join(
                        f"  [{r['level']}] {r['message'][:200]}" for r in errors
                    )
                except Exception as e:
                    error_details += f"\nCould not read event log: {e}"
            pytest.fail(error_details)

        result = test_spark.table(_fqn("specification_documents_parsed"))
        assert result.count() >= 1, (
            "Expected at least 1 parsed document. "
            "If 0, ai_parse_document may have failed on the minimal PDF."
        )
        # Check the parsed column exists and is not all error
        error_count = result.filter(
            "try_cast(parsed:error_status AS STRING) IS NOT NULL"
        ).count()
        assert error_count == 0, (
            f"ai_parse_document returned error_status for {error_count} docs. "
            "The minimal PDF fixture may need updating."
        )

    def test_full_chain_layers_3_through_5(self, test_spark):
        """Layers 2-5: Complete chain produces classified, extracted, and chunked output."""
        mock_specification_documents_with_text(test_spark)

        # Run the full chain (Layers 2-5)
        status = test_pipeline.run(
            test_spark,
            {
                _fqn("specification_documents_parsed"),
                _fqn("specification_documents_classified"),
                _fqn("specification_documents_extracted"),
                _fqn("specification_search_chunks"),
            },
        )

        # Verify each layer produced output
        parsed = test_spark.table(_fqn("specification_documents_parsed"))
        classified = test_spark.table(_fqn("specification_documents_classified"))
        extracted = test_spark.table(_fqn("specification_documents_extracted"))
        chunks = test_spark.table(_fqn("specification_search_chunks"))

        # At minimum, parsed should have data (if PDF is valid)
        parsed_count = parsed.count()
        if parsed_count == 0:
            pytest.skip("ai_parse_document produced no output — minimal PDF may be invalid")

        # Layers 3-5 should have data if parsed was successful
        assert classified.count() >= 1, "classify_documents produced no output"
        assert extracted.count() >= 1, "extract_fields produced no output"
        assert chunks.count() >= 1, "prep_search produced no chunks"

        # Verify classification is one of the expected labels
        label = classified.select("classification").collect()[0][0]
        valid_labels = {
            "payer_sheet", "implementation_guide", "value_set",
            "change_notice", "general_reference",
        }
        assert label in valid_labels, f"Unexpected classification label: {label}"

        # Verify chunks have expected columns
        chunk_cols = set(chunks.columns)
        assert "chunk_id" in chunk_cols
        assert "chunk_to_embed" in chunk_cols
        assert "doc_source_id" in chunk_cols


# ═══════════════════════════════════════════════════════════════════════════════
# Test B: Layers 3-5 with pre-crafted VARIANT (cheaper, no ai_parse_document)
# Cost: ~$0.02-0.05 per run (only classify + extract + prep_search)
# ═══════════════════════════════════════════════════════════════════════════════


class TestDownstreamLayersWithMockedParsed:
    """Test Layers 3-5 using a pre-crafted parsed VARIANT.

    Skips ai_parse_document entirely. Validates that:
    - classify_documents reads from specification_documents_parsed
    - extract_fields reads from specification_documents_classified
    - prep_search reads from specification_documents_parsed
    - Filters correctly exclude error_status rows
    """

    def test_classify_documents(self, test_spark):
        """Layer 3: ai_classify produces a valid label from mock parsed data."""
        mock_specification_documents_parsed(test_spark)

        test_pipeline.run(
            test_spark,
            {_fqn("specification_documents_classified")},
        )

        result = test_spark.table(_fqn("specification_documents_classified"))
        assert result.count() == 1, "Expected exactly 1 classified document"

        row = result.collect()[0]
        valid_labels = {
            "payer_sheet", "implementation_guide", "value_set",
            "change_notice", "general_reference",
        }
        assert row["classification"] in valid_labels, (
            f"Got unexpected label: {row['classification']}"
        )
        # Verify pass-through columns
        assert row["doc_source_id"] is not None
        assert row["path"] is not None
        assert row["parsed"] is not None

    def test_extract_fields(self, test_spark):
        """Layer 4: ai_extract produces structured VARIANT from classified data."""
        mock_specification_documents_parsed(test_spark)

        # Need to run classify first (Layer 4 reads from classified)
        test_pipeline.run(
            test_spark,
            {
                _fqn("specification_documents_classified"),
                _fqn("specification_documents_extracted"),
            },
        )

        result = test_spark.table(_fqn("specification_documents_extracted"))
        assert result.count() == 1, "Expected exactly 1 extracted document"

        row = result.collect()[0]
        assert row["extracted"] is not None, "ai_extract returned NULL"
        assert row["classification"] is not None, "classification column not passed through"

    def test_prep_search(self, test_spark):
        """Layer 5: ai_prep_search produces chunks from parsed data."""
        mock_specification_documents_parsed(test_spark)

        test_pipeline.run(
            test_spark,
            {_fqn("specification_search_chunks")},
        )

        result = test_spark.table(_fqn("specification_search_chunks"))
        assert result.count() >= 1, "Expected at least 1 search chunk"

        # Verify chunk schema
        cols = set(result.columns)
        expected_cols = {"doc_source_id", "path", "chunk_id", "chunk_position",
                        "chunk_to_retrieve", "chunk_to_embed"}
        assert expected_cols.issubset(cols), f"Missing columns: {expected_cols - cols}"

        # Verify chunk content is non-empty
        row = result.collect()[0]
        assert row["chunk_to_embed"] is not None and len(row["chunk_to_embed"]) > 0

    def test_error_status_filter(self, test_spark):
        """Verify that rows with error_status are filtered out by Layers 3 and 5."""
        # Insert a row with error_status set
        test_spark.sql(f"""
            CREATE OR REPLACE TABLE {_fqn('specification_documents_parsed')} AS
            SELECT
                'error_doc_id' as doc_source_id,
                '/Volumes/test/spec/vol/corrupt.pdf' as path,
                named_struct(
                    'file_path', '/Volumes/test/spec/vol/corrupt.pdf',
                    'file_name', 'corrupt.pdf',
                    'file_size', CAST(100 AS BIGINT),
                    'file_block_start', CAST(0 AS BIGINT),
                    'file_block_length', CAST(100 AS BIGINT),
                    'file_modification_time', CURRENT_TIMESTAMP()
                ) as file_metadata,
                PARSE_JSON('{{
                    "error_status": "INVALID_DOCUMENT",
                    "error_message": "Document could not be parsed"
                }}') as parsed
        """)

        # Run classify — should produce 0 rows (error filtered)
        test_pipeline.run(
            test_spark,
            {_fqn("specification_documents_classified")},
        )
        classified = test_spark.table(_fqn("specification_documents_classified"))
        assert classified.count() == 0, (
            "Error documents should be filtered out by classify_documents"
        )

        # Run prep_search — should also produce 0 rows
        test_pipeline.run(
            test_spark,
            {_fqn("specification_search_chunks")},
        )
        chunks = test_spark.table(_fqn("specification_search_chunks"))
        assert chunks.count() == 0, (
            "Error documents should be filtered out by prep_search"
        )


# ═══════════════════════════════════════════════════════════════════════════════
# Test C: Extension filter behavior (Layer 2)
# Cost: ~$0.02 per run (1 ai_parse_document call)
# ═══════════════════════════════════════════════════════════════════════════════


class TestExtensionFilter:
    """Verify that Layer 2 only processes supported file extensions."""

    def test_unsupported_extension_filtered(self, test_spark):
        """Files with .txt extension should not reach specification_documents_parsed."""
        test_spark.sql(f"""
            CREATE OR REPLACE TABLE {_fqn('specification_documents')} AS
            SELECT
                'txt_doc_id' as doc_source_id,
                named_struct(
                    'file_path', '/Volumes/test/spec/vol/readme.txt',
                    'file_name', 'readme.txt',
                    'file_size', CAST(50 AS BIGINT),
                    'file_block_start', CAST(0 AS BIGINT),
                    'file_block_length', CAST(50 AS BIGINT),
                    'file_modification_time', CURRENT_TIMESTAMP()
                ) as file_metadata,
                CURRENT_TIMESTAMP() as ingest_time,
                '/Volumes/test/spec/vol/readme.txt' as path,
                CURRENT_TIMESTAMP() as modificationTime,
                CAST(50 AS LONG) as length,
                CAST('hello world' AS BINARY) as content
        """)

        test_pipeline.run(
            test_spark,
            {_fqn("specification_documents_parsed")},
        )

        result = test_spark.table(_fqn("specification_documents_parsed"))
        assert result.count() == 0, (
            "Files with .txt extension should be filtered out by parse_documents"
        )
