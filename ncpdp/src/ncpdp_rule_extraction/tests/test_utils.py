"""Unit tests for NCPDP rule extraction utility functions."""

import sys
sys.path.insert(0, "..")
from utilities.utils import (
    generate_rule_id,
    field_code_to_bronze_key,
    normalize_column_name,
    generate_column_comment,
    resolve_condition_segment,
)


def test_generate_rule_id_deterministic():
    """Same inputs always produce same rule_id."""
    id1 = generate_rule_id("07", "407-D7", "MANDATORY", None, ["B1", "B3"])
    id2 = generate_rule_id("07", "407-D7", "MANDATORY", None, ["B3", "B1"])
    assert id1 == id2, "Rule IDs should be deterministic regardless of tx type order"


def test_generate_rule_id_different_inputs():
    """Different inputs produce different rule_ids."""
    id1 = generate_rule_id("07", "407-D7", "MANDATORY", None, ["B1"])
    id2 = generate_rule_id("07", "407-D7", "FORMAT", None, ["B1"])
    assert id1 != id2


def test_field_code_to_bronze_key():
    """Field codes convert to bronze key format."""
    assert field_code_to_bronze_key("101-A1") == "F_101_A1"
    assert field_code_to_bronze_key("407-D7") == "F_407_D7"
    assert field_code_to_bronze_key("450-EF") == "F_450_EF"
    assert field_code_to_bronze_key(None) is None


def test_normalize_column_name():
    """Field names normalize to snake_case."""
    assert normalize_column_name("BIN NUMBER") == "bin_number"
    assert normalize_column_name("PRODUCT/SERVICE ID") == "product_service_id"
    assert normalize_column_name("Date of Service") == "date_of_service"
    assert normalize_column_name(None) is None


def test_normalize_column_name_truncates():
    """Long names are truncated to 63 chars."""
    long_name = "A" * 100
    result = normalize_column_name(long_name)
    assert len(result) <= 63


def test_generate_column_comment():
    """Column comments follow expected format."""
    result = generate_column_comment("101-A1", "BIN NUMBER", "M")
    assert "NCPDP Field 101-A1" in result
    assert "BIN NUMBER" in result
    assert "Usage: M" in result


def test_generate_column_comment_no_usage():
    """Comment works without payer_usage."""
    result = generate_column_comment("101-A1", "BIN NUMBER", None)
    assert "NCPDP Field 101-A1" in result
    assert "Usage" not in result


def test_resolve_condition_segment_cross_ref():
    """Cross-segment references are resolved."""
    # F_406_D6 lives in segment 07 (Claim)
    result = resolve_condition_segment("F_406_D6 = '2'", "10")  # Rule in segment 10 (Compound)
    assert result == "07"


def test_resolve_condition_segment_same_segment():
    """Same-segment references return None."""
    result = resolve_condition_segment("F_406_D6 = '2'", "07")  # Rule IS in segment 07
    assert result is None


def test_resolve_condition_segment_null_condition():
    """Null condition returns None."""
    assert resolve_condition_segment(None, "07") is None


if __name__ == "__main__":
    test_generate_rule_id_deterministic()
    test_generate_rule_id_different_inputs()
    test_field_code_to_bronze_key()
    test_normalize_column_name()
    test_normalize_column_name_truncates()
    test_generate_column_comment()
    test_generate_column_comment_no_usage()
    test_resolve_condition_segment_cross_ref()
    test_resolve_condition_segment_same_segment()
    test_resolve_condition_segment_null_condition()
    print("All 10 tests PASSED")
