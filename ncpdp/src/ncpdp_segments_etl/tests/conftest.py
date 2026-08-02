"""
Shared fixtures for ncpdp_segments_etl pipeline tests.

Uses the SDP testing framework (pyspark.pipelines.testing) to provide
isolated test execution with catalog-table redirection.
"""
import pytest
from pyspark.pipelines.testing import TestPipeline, test_spark  # noqa: F401


# ─── Pipeline reference ──────────────────────────────────────────────────────
test_pipeline = TestPipeline.active()

# Resolved from pipeline configuration
CATALOG = "ncpdp_dev"
SCHEMA = "dev_matthew_giglia_rx_claims"


def fqn(table: str) -> str:
    """Build fully-qualified table name matching pipeline references."""
    return f"{CATALOG}.{SCHEMA}.{table}"


# ─── Mock Data Fixtures ──────────────────────────────────────────────────────

@pytest.fixture
def mock_specification_rules(test_spark):
    """
    Create a minimal specification_rules table with known rules for
    segments 07 (Claim) and 00 (Header).

    Covers rule_types: MANDATORY, ALLOWED_VALUES, FORMAT, SITUATIONAL
    """
    test_spark.sql(f"""
        CREATE TABLE {fqn('specification_rules')} AS
        SELECT * FROM VALUES
            -- Claim segment (07): MANDATORY fields
            ('r001', '07', 'Claim', 'FIELD', 'F_407_D7', 'product_service_id', 'STRING', 'MANDATORY',
             CAST(NULL AS ARRAY<STRING>), CAST(NULL AS STRING), CAST(NULL AS STRING),
             'NCPDP Field 407-D7: PRODUCT/SERVICE ID'),
            ('r002', '07', 'Claim', 'FIELD', 'F_442_E7', 'quantity_dispensed', 'DECIMAL(10,3)', 'MANDATORY',
             CAST(NULL AS ARRAY<STRING>), CAST(NULL AS STRING), CAST(NULL AS STRING),
             'NCPDP Field 442-E7: QUANTITY DISPENSED'),
            ('r003', '07', 'Claim', 'FIELD', 'F_405_D5', 'days_supply', 'INT', 'MANDATORY',
             CAST(NULL AS ARRAY<STRING>), CAST(NULL AS STRING), CAST(NULL AS STRING),
             'NCPDP Field 405-D5: DAYS SUPPLY'),
            -- Claim segment (07): ALLOWED_VALUES
            ('r004', '07', 'Claim', 'FIELD', 'F_406_D6', 'compound_code', 'STRING', 'ALLOWED_VALUES',
             ARRAY('1', '2'), CAST(NULL AS STRING), CAST(NULL AS STRING),
             'NCPDP Field 406-D6: COMPOUND CODE'),
            -- Claim segment (07): FORMAT
            ('r005', '07', 'Claim', 'FIELD', 'F_402_D2', 'prescription_reference_number', 'STRING', 'FORMAT',
             CAST(NULL AS ARRAY<STRING>), '^[0-9]+, CAST(NULL AS STRING),
             'NCPDP Field 402-D2: Rx REFERENCE NUMBER'),
            -- Claim segment (07): SITUATIONAL (no expectation generated)
            ('r006', '07', 'Claim', 'FIELD', 'F_414_DE', 'date_prescription_written', 'DATE', 'SITUATIONAL',
             CAST(NULL AS ARRAY<STRING>), CAST(NULL AS STRING), CAST(NULL AS STRING),
             'NCPDP Field 414-DE: DATE PRESCRIPTION WRITTEN'),
            -- Header segment (00): MANDATORY
            ('r007', '00', 'Transaction Header', 'FIELD', 'F_101_A1', 'bin_number', 'STRING', 'MANDATORY',
             CAST(NULL AS ARRAY<STRING>), CAST(NULL AS STRING), CAST(NULL AS STRING),
             'NCPDP Field 101-A1: BIN NUMBER'),
            ('r008', '00', 'Transaction Header', 'FIELD', 'F_103_A3', 'transaction_code', 'STRING', 'ALLOWED_VALUES',
             ARRAY('B1', 'B2', 'B3', 'E1'), CAST(NULL AS STRING), CAST(NULL AS STRING),
             'NCPDP Field 103-A3: TRANSACTION CODE'),
            -- TRANSACTION-level rule (should be excluded from FIELD processing)
            ('r009', '07', 'Claim', 'TRANSACTION', 'F_407_D7', 'product_service_id', 'STRING', 'MANDATORY',
             CAST(NULL AS ARRAY<STRING>), CAST(NULL AS STRING), CAST(NULL AS STRING), NULL)
        AS t(
            rule_id, segment_code, segment_name, rule_level, bronze_key,
            column_name, data_type, rule_type, allowed_values, format_pattern,
            condition, column_comment
        )
    """)


@pytest.fixture
def mock_bronze_requests(test_spark):
    """
    Create mock claimbilling_bronze_requests with key-value rows for
    Claim (S_07) and Header (S_HD) segments.

    Includes:
    - 2 complete transactions (file_001/pos=0, file_002/pos=0)
    - 1 transaction with NULL mandatory field (file_003/pos=0)
    - 1 transaction with invalid allowed_value (file_004/pos=0)
    """
    test_spark.sql(f"""
        CREATE TABLE {fqn('claimbilling_bronze_requests')} AS
        SELECT * FROM VALUES
            -- ═══ Transaction 1: Valid claim (file_001, pos=0) ═══
            ('file_001', 'S_07', 0, 'F_407_D7', CAST('12345678901' AS VARIANT)),
            ('file_001', 'S_07', 0, 'F_442_E7', CAST('30.000' AS VARIANT)),
            ('file_001', 'S_07', 0, 'F_405_D5', CAST('30' AS VARIANT)),
            ('file_001', 'S_07', 0, 'F_406_D6', CAST('1' AS VARIANT)),
            ('file_001', 'S_07', 0, 'F_402_D2', CAST('999888777' AS VARIANT)),
            ('file_001', 'S_07', 0, 'F_414_DE', CAST('2026-01-15' AS VARIANT)),
            -- Header for file_001
            ('file_001', 'S_HD', 0, 'F_101_A1', CAST('999999' AS VARIANT)),
            ('file_001', 'S_HD', 0, 'F_103_A3', CAST('B1' AS VARIANT)),
            -- Patient + Insurance presence (for transaction validation)
            ('file_001', 'S_01', 0, 'F_305_C5', CAST('1' AS VARIANT)),
            ('file_001', 'S_04', 0, 'F_302_C2', CAST('CARD001' AS VARIANT)),

            -- ═══ Transaction 2: Valid claim (file_002, pos=0) ═══
            ('file_002', 'S_07', 0, 'F_407_D7', CAST('98765432101' AS VARIANT)),
            ('file_002', 'S_07', 0, 'F_442_E7', CAST('60.500' AS VARIANT)),
            ('file_002', 'S_07', 0, 'F_405_D5', CAST('90' AS VARIANT)),
            ('file_002', 'S_07', 0, 'F_406_D6', CAST('2' AS VARIANT)),
            ('file_002', 'S_07', 0, 'F_402_D2', CAST('111222333' AS VARIANT)),
            ('file_002', 'S_07', 0, 'F_414_DE', CAST('2026-03-20' AS VARIANT)),
            -- Header for file_002
            ('file_002', 'S_HD', 0, 'F_101_A1', CAST('888888' AS VARIANT)),
            ('file_002', 'S_HD', 0, 'F_103_A3', CAST('B1' AS VARIANT)),
            -- All mandatory segments present + compound segment (compound_code=2)
            ('file_002', 'S_01', 0, 'F_305_C5', CAST('2' AS VARIANT)),
            ('file_002', 'S_04', 0, 'F_302_C2', CAST('CARD002' AS VARIANT)),
            ('file_002', 'S_10', 0, 'F_447_EC', CAST('5' AS VARIANT)),

            -- ═══ Transaction 3: Missing mandatory field (file_003, pos=0) ═══
            -- product_service_id is NULL (F_407_D7 missing)
            ('file_003', 'S_07', 0, 'F_442_E7', CAST('10.000' AS VARIANT)),
            ('file_003', 'S_07', 0, 'F_405_D5', CAST('7' AS VARIANT)),
            ('file_003', 'S_07', 0, 'F_406_D6', CAST('1' AS VARIANT)),
            ('file_003', 'S_07', 0, 'F_402_D2', CAST('555666777' AS VARIANT)),
            -- Header for file_003
            ('file_003', 'S_HD', 0, 'F_101_A1', CAST('777777' AS VARIANT)),
            ('file_003', 'S_HD', 0, 'F_103_A3', CAST('E1' AS VARIANT)),
            ('file_003', 'S_01', 0, 'F_305_C5', CAST('1' AS VARIANT)),
            ('file_003', 'S_04', 0, 'F_302_C2', CAST('CARD003' AS VARIANT)),

            -- ═══ Transaction 4: Invalid allowed_value (file_004, pos=0) ═══
            ('file_004', 'S_07', 0, 'F_407_D7', CAST('44444444444' AS VARIANT)),
            ('file_004', 'S_07', 0, 'F_442_E7', CAST('1.000' AS VARIANT)),
            ('file_004', 'S_07', 0, 'F_405_D5', CAST('1' AS VARIANT)),
            ('file_004', 'S_07', 0, 'F_406_D6', CAST('9' AS VARIANT)),  -- INVALID: not in ['1','2']
            ('file_004', 'S_07', 0, 'F_402_D2', CAST('ABC123' AS VARIANT)),  -- INVALID FORMAT: not numeric
            ('file_004', 'S_07', 0, 'F_414_DE', CAST('2026-07-01' AS VARIANT)),
            -- Header with invalid transaction_code
            ('file_004', 'S_HD', 0, 'F_101_A1', CAST('666666' AS VARIANT)),
            ('file_004', 'S_HD', 0, 'F_103_A3', CAST('XX' AS VARIANT)),  -- INVALID: not in ['B1','B2','B3','E1']
            ('file_004', 'S_01', 0, 'F_305_C5', CAST('1' AS VARIANT)),
            ('file_004', 'S_04', 0, 'F_302_C2', CAST('CARD004' AS VARIANT))
        AS t(
            transaction_file_source_id, request_segment, request_pos,
            key, value
        )
    """)


@pytest.fixture
def mock_all(mock_specification_rules, mock_bronze_requests):
    """Convenience fixture that sets up both mock tables."""
    pass
