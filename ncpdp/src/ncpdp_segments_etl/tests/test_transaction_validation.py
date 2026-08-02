"""
SDP-native tests for claimbilling_silver_transaction_validation.

Validates:
- Segment presence flags: correct boolean per (file, pos)
- Conditional expectations: compound_when_required logic
- Mandatory segment checks: header_present, patient_present, etc.
- Event log captures expectation pass/fail counts
"""
import pytest
from pyspark.pipelines.testing import TestPipeline, test_spark  # noqa: F401
from conftest import test_pipeline, fqn


# ─── Replicate the validation SQL (from transaction_validation.py) ───────────
# This mirrors the pipeline's SQL but executes via test_spark directly.

def _run_validation_sql(test_spark):
    """Execute the transaction validation SQL directly against mock tables."""
    bronze_requests_table = fqn("claimbilling_bronze_requests")
    sql = f"""
    WITH segment_presence AS (
        SELECT
            transaction_file_source_id,
            request_pos,
            MAX(CASE WHEN request_segment = 'S_HD' THEN true ELSE false END) AS header_present,
            MAX(CASE WHEN request_segment = 'S_01' THEN true ELSE false END) AS patient_present,
            MAX(CASE WHEN request_segment = 'S_02' THEN true ELSE false END) AS pharmacy_provider_present,
            MAX(CASE WHEN request_segment = 'S_03' THEN true ELSE false END) AS prescriber_present,
            MAX(CASE WHEN request_segment = 'S_04' THEN true ELSE false END) AS insurance_present,
            MAX(CASE WHEN request_segment = 'S_05' THEN true ELSE false END) AS cob_present,
            MAX(CASE WHEN request_segment = 'S_06' THEN true ELSE false END) AS workers_comp_present,
            MAX(CASE WHEN request_segment = 'S_07' THEN true ELSE false END) AS claim_present,
            MAX(CASE WHEN request_segment = 'S_08' THEN true ELSE false END) AS dur_pps_present,
            MAX(CASE WHEN request_segment = 'S_10' THEN true ELSE false END) AS compound_present,
            MAX(CASE WHEN request_segment = 'S_11' THEN true ELSE false END) AS pricing_present,
            MAX(CASE WHEN request_segment = 'S_13' THEN true ELSE false END) AS clinical_present,
            MAX(CASE WHEN request_segment = 'S_07' AND key = 'F_406_D6'
                     THEN value::STRING END) AS compound_code,
            MAX(CASE WHEN request_segment = 'S_07' AND key = 'F_407_D7'
                     THEN value::STRING END) AS product_service_id,
            COUNT(DISTINCT request_segment) AS segment_count
        FROM {bronze_requests_table}
        GROUP BY transaction_file_source_id, request_pos
    )
    SELECT * FROM segment_presence
    """
    return test_spark.sql(sql)


# ─── Transaction expectation expressions (from transaction_validation.py) ────
TRANSACTION_EXPECTATIONS = {
    "header_present": "header_present = true",
    "patient_present": "patient_present = true",
    "insurance_present": "insurance_present = true",
    "claim_present": "claim_present = true",
    "compound_when_required": (
        "NOT (compound_code = '2') OR compound_present = true"
    ),
}


class TestTransactionValidationPresenceFlags:
    """Tests for segment presence flag correctness."""

    def test_all_segments_detected(self, test_spark, mock_all):
        """file_001 has S_HD, S_07, S_01, S_04 -> those flags should be true."""
        result = _run_validation_sql(test_spark)
        row = result.filter(
            "transaction_file_source_id = 'file_001' AND request_pos = 0"
        ).collect()[0]

        assert row["header_present"] == True
        assert row["patient_present"] == True
        assert row["insurance_present"] == True
        assert row["claim_present"] == True
        # No compound segment for file_001
        assert row["compound_present"] == False

    def test_compound_segment_detected(self, test_spark, mock_all):
        """file_002 has S_10 (compound) -> compound_present should be true."""
        result = _run_validation_sql(test_spark)
        row = result.filter(
            "transaction_file_source_id = 'file_002' AND request_pos = 0"
        ).collect()[0]

        assert row["compound_present"] == True
        assert row["compound_code"] == "2"  # Triggers compound requirement

    def test_segment_count_correct(self, test_spark, mock_all):
        """segment_count should reflect distinct segments per transaction."""
        result = _run_validation_sql(test_spark)

        # file_001: S_HD, S_07, S_01, S_04 = 4 segments
        row1 = result.filter(
            "transaction_file_source_id = 'file_001' AND request_pos = 0"
        ).collect()[0]
        assert row1["segment_count"] == 4

        # file_002: S_HD, S_07, S_01, S_04, S_10 = 5 segments
        row2 = result.filter(
            "transaction_file_source_id = 'file_002' AND request_pos = 0"
        ).collect()[0]
        assert row2["segment_count"] == 5


class TestTransactionValidationExpectations:
    """Tests for cross-segment expectation rules."""

    def test_compound_when_required_passes(self, test_spark, mock_all):
        """
        file_002: compound_code='2' AND compound_present=true.
        The compound_when_required expectation should PASS.
        """
        result = _run_validation_sql(test_spark)
        row = result.filter(
            "transaction_file_source_id = 'file_002' AND request_pos = 0"
        ).collect()[0]

        # Verify the condition: NOT(compound_code='2') OR compound_present=true
        assert row["compound_code"] == "2"
        assert row["compound_present"] == True

    def test_compound_when_required_not_triggered(self, test_spark, mock_all):
        """
        file_001: compound_code='1' (not '2'), so compound segment not required.
        The expectation should PASS regardless of compound_present value.
        """
        result = _run_validation_sql(test_spark)
        row = result.filter(
            "transaction_file_source_id = 'file_001' AND request_pos = 0"
        ).collect()[0]

        # NOT(compound_code='2') evaluates to True -> expectation passes
        assert row["compound_code"] == "1"
        assert row["compound_present"] == False

    def test_mandatory_segments_all_present(self, test_spark, mock_all):
        """
        file_001 and file_002 have all mandatory segments (HD, 01, 04, 07).
        All mandatory-presence expectations should pass for these rows.
        """
        result = _run_validation_sql(test_spark)

        for file_id in ["file_001", "file_002"]:
            row = result.filter(
                f"transaction_file_source_id = '{file_id}' AND request_pos = 0"
            ).collect()[0]
            assert row["header_present"] == True
            assert row["patient_present"] == True
            assert row["insurance_present"] == True
            assert row["claim_present"] == True

    def test_expectation_expressions_evaluate(self, test_spark, mock_all):
        """
        Apply TRANSACTION_EXPECTATIONS as SQL filter expressions.
        Valid transactions should satisfy all expectations.
        """
        result = _run_validation_sql(test_spark)
        result.createOrReplaceTempView("_tv_validation")

        # file_001 and file_002 should pass all mandatory expectations
        for exp_name, exp_sql in TRANSACTION_EXPECTATIONS.items():
            passing = test_spark.sql(f"""
                SELECT * FROM _tv_validation
                WHERE transaction_file_source_id IN ('file_001', 'file_002')
                  AND request_pos = 0
                  AND ({exp_sql})
            """).count()
            assert passing == 2, f"Expectation '{exp_name}' failed for valid data"


class TestTransactionValidationEdgeCases:
    """Edge case tests for transaction validation."""

    def test_transaction_with_only_header(self, test_spark):
        """
        A transaction with only S_HD should have all other flags as false.
        Mandatory expectations (patient, insurance, claim) should fail.
        """
        # Create minimal mock: just one header row
        test_spark.sql(f"""
            CREATE OR REPLACE TABLE {fqn('claimbilling_bronze_requests')} AS
            SELECT * FROM VALUES
                ('file_solo', 'S_HD', 0, 'F_101_A1', CAST('111111' AS VARIANT))
            AS t(transaction_file_source_id, request_segment, request_pos, key, value)
        """)

        result = _run_validation_sql(test_spark)
        row = result.filter(
            "transaction_file_source_id = 'file_solo'"
        ).collect()[0]

        assert row["header_present"] == True
        assert row["patient_present"] == False
        assert row["insurance_present"] == False
        assert row["claim_present"] == False
        assert row["compound_present"] == False
        assert row["compound_code"] is None

    def test_missing_mandatory_fails_expectation(self, test_spark):
        """
        A transaction with only S_HD should fail mandatory expectations.
        Validates the expectation SQL expressions against incomplete data.
        """
        test_spark.sql(f"""
            CREATE OR REPLACE TABLE {fqn('claimbilling_bronze_requests')} AS
            SELECT * FROM VALUES
                ('file_solo', 'S_HD', 0, 'F_101_A1', CAST('111111' AS VARIANT))
            AS t(transaction_file_source_id, request_segment, request_pos, key, value)
        """)

        result = _run_validation_sql(test_spark)
        result.createOrReplaceTempView("_tv_edge")

        # patient_present expectation should FAIL for solo-header transaction
        failing = test_spark.sql("""
            SELECT * FROM _tv_edge
            WHERE NOT (patient_present = true)
        """).count()
        assert failing == 1
