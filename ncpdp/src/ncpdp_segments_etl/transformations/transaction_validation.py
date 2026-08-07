"""
ncpdp_segments_etl — Transaction-Level Validation

Cross-segment validation: checks that required segments are present
for each transaction based on TRANSACTION-level rules from specification_rules.

Produces:
  - claimbilling_silver_transaction_validation: one row per transaction
    with segment-presence flags and expectation checks
"""
from pyspark import pipelines as dp
from utilities.utils import SILVER_TABLE_PROPERTIES

catalog = spark.conf.get("catalog_use")
schema = spark.conf.get("schema_use")
bronze_requests_table = f"{catalog}.{schema}.claimbilling_bronze_requests"

# ─── Transaction-level validation expectations ─────────────────────────────
# These are derived from TRANSACTION-level rules in specification_rules.
# Core NCPDP requirement: Header (S_HD), Patient (S_01), Insurance (S_04),
# and Claim (S_07) segments are mandatory for every claim billing transaction.

TRANSACTION_EXPECTATIONS = {
    "header_present": "header_present = true",
    "patient_present": "patient_present = true",
    "insurance_present": "insurance_present = true",
    "claim_present": "claim_present = true",
    "compound_when_required": (
        "NOT (compound_code = '2') OR compound_present = true"
    ),
}


@dp.table(
    name=f"{catalog}.{schema}.claimbilling_silver_transaction_validation",
    table_properties=SILVER_TABLE_PROPERTIES,
    cluster_by_auto=True,
)
@dp.expect_all(TRANSACTION_EXPECTATIONS)
def transaction_validation():
    """
    Build a transaction-level summary:
    - Which segments are present per (transaction_file_source_id, request_pos)
    - Key field values that trigger conditional segment requirements
    - Validation flags for expectation checks
    """
    sql = f"""
    WITH segment_presence AS (
        SELECT
            transaction_file_source_id,
            request_pos,
            -- Segment presence flags
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
            -- Key conditional trigger fields (from Claim segment)
            MAX(CASE WHEN request_segment = 'S_07' AND key = 'F_406_D6'
                     THEN value::STRING END) AS compound_code,
            MAX(CASE WHEN request_segment = 'S_07' AND key = 'F_407_D7'
                     THEN value::STRING END) AS product_service_id,
            -- Count of segments per transaction
            COUNT(DISTINCT request_segment) AS segment_count
        FROM {bronze_requests_table}
        GROUP BY transaction_file_source_id, request_pos
    )
    SELECT * FROM segment_presence
    """
    return spark.sql(sql)
