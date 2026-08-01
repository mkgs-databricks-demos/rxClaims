"""Unit tests for ncpdp_document_intelligence.utilities.utils pure functions.

Tier 2 tests — no Spark dependency, no AI function calls, fast execution.
Run with:
    PYTHONDONTWRITEBYTECODE=1 python -m pytest \
        src/ncpdp_document_intelligence/tests/test_utils.py \
        -v -p no:cacheprovider
"""

import json
import sys
from types import ModuleType
from unittest.mock import MagicMock

import pytest

# ---------------------------------------------------------------------------
# Mock pyspark modules that aren't available outside SDP runtime.
# We only test pure functions (no Spark dependency), but the module-level
# imports in utils.py pull in pyspark.pipelines and pyspark.sql.functions.
# ---------------------------------------------------------------------------
_mock_pipelines = ModuleType("pyspark.pipelines")
_mock_pipelines.table = MagicMock()
_mock_pipelines.materialized_view = MagicMock()
_mock_pipelines.temporary_view = MagicMock()

_mock_sql_functions = ModuleType("pyspark.sql.functions")
_mock_sql_functions.col = MagicMock()
_mock_sql_functions.lower = MagicMock()
_mock_sql_functions.regexp_extract = MagicMock()

_mock_sql = ModuleType("pyspark.sql")
_mock_sql.SparkSession = MagicMock()

_mock_pyspark = ModuleType("pyspark")
_mock_pyspark.pipelines = _mock_pipelines
_mock_pyspark.sql = _mock_sql

sys.modules.setdefault("pyspark", _mock_pyspark)
sys.modules.setdefault("pyspark.pipelines", _mock_pipelines)
sys.modules.setdefault("pyspark.sql", _mock_sql)
sys.modules.setdefault("pyspark.sql.functions", _mock_sql_functions)

# Now safe to import the module under test
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

from ncpdp_document_intelligence.utilities.utils import (  # noqa: E402
    resolve_volume_path,
    resolve_image_output_path,
    is_supported_extension,
    get_table_name,
    get_table_properties,
    validate_classification_labels,
    validate_extraction_schema,
    build_parse_document_expr,
    build_classify_expr,
    build_extract_expr,
    build_prep_search_explode_expr,
    default_table_properties,
    SUPPORTED_EXTENSIONS,
    DOCUMENT_CLASSIFICATION_LABELS,
    DOCUMENT_EXTRACTION_SCHEMA,
    EXTRACTION_INSTRUCTIONS,
)


# ═══════════════════════════════════════════════════════════════════════════════
# resolve_volume_path
# ═══════════════════════════════════════════════════════════════════════════════


class TestResolveVolumePath:
    def test_basic_path(self):
        result = resolve_volume_path("my_catalog", "my_schema", "my_volume")
        assert result == "/Volumes/my_catalog/my_schema/my_volume"

    def test_with_sub_path(self):
        result = resolve_volume_path("cat", "sch", "vol", "subdir/nested")
        assert result == "/Volumes/cat/sch/vol/subdir/nested"

    def test_none_sub_path(self):
        result = resolve_volume_path("cat", "sch", "vol", None)
        assert result == "/Volumes/cat/sch/vol"

    def test_empty_string_sub_path(self):
        """Empty string is falsy, so should behave like None."""
        result = resolve_volume_path("cat", "sch", "vol", "")
        assert result == "/Volumes/cat/sch/vol"


# ═══════════════════════════════════════════════════════════════════════════════
# resolve_image_output_path
# ═══════════════════════════════════════════════════════════════════════════════


class TestResolveImageOutputPath:
    def test_default_fallback(self):
        result = resolve_image_output_path("cat", "sch", "vol")
        assert result == "/Volumes/cat/sch/vol/parsed_images"

    def test_with_custom_sub_path(self):
        result = resolve_image_output_path("cat", "sch", "vol", "custom/images")
        assert result == "/Volumes/cat/sch/vol/custom/images"

    def test_none_image_sub_path(self):
        result = resolve_image_output_path("cat", "sch", "vol", None)
        assert result == "/Volumes/cat/sch/vol/parsed_images"


# ═══════════════════════════════════════════════════════════════════════════════
# is_supported_extension
# ═══════════════════════════════════════════════════════════════════════════════


class TestIsSupportedExtension:
    @pytest.mark.parametrize("ext", SUPPORTED_EXTENSIONS)
    def test_all_supported_extensions(self, ext):
        assert is_supported_extension(f"/path/to/file{ext}") is True

    @pytest.mark.parametrize("ext", [".PDF", ".Jpg", ".DOCX", ".Pptx"])
    def test_case_insensitive(self, ext):
        assert is_supported_extension(f"/path/to/file{ext}") is True

    @pytest.mark.parametrize("ext", [".txt", ".csv", ".html", ".xml", ".zip"])
    def test_unsupported_extensions(self, ext):
        assert is_supported_extension(f"/path/to/file{ext}") is False

    def test_no_extension(self):
        assert is_supported_extension("/path/to/Makefile") is False

    def test_hidden_file_no_ext(self):
        """Dotfiles like .gitignore — regex captures .gitignore as extension."""
        assert is_supported_extension("/path/.gitignore") is False

    def test_double_extension(self):
        """Only the last extension matters: .tar.gz → .gz (unsupported)."""
        assert is_supported_extension("/path/file.tar.gz") is False

    def test_double_extension_supported(self):
        """file.backup.pdf → .pdf (supported)."""
        assert is_supported_extension("/path/file.backup.pdf") is True

    def test_empty_string(self):
        assert is_supported_extension("") is False


# ═══════════════════════════════════════════════════════════════════════════════
# get_table_name
# ═══════════════════════════════════════════════════════════════════════════════


class TestGetTableName:
    def test_basic(self):
        assert get_table_name("cat", "sch", "my_table") == "cat.sch.my_table"

    def test_with_underscores(self):
        result = get_table_name("ncpdp_dev", "dev_rx_claims", "specification_documents_parsed")
        assert result == "ncpdp_dev.dev_rx_claims.specification_documents_parsed"


# ═══════════════════════════════════════════════════════════════════════════════
# get_table_properties
# ═══════════════════════════════════════════════════════════════════════════════


class TestGetTableProperties:
    def test_adds_quality(self):
        base = {"delta.enableChangeDataFeed": "true"}
        result = get_table_properties(base, "silver")
        assert result["quality"] == "silver"
        assert result["delta.enableChangeDataFeed"] == "true"

    def test_does_not_mutate_original(self):
        base = {"key": "value"}
        get_table_properties(base, "gold")
        assert "quality" not in base

    def test_overwrites_existing_quality(self):
        base = {"quality": "bronze"}
        result = get_table_properties(base, "gold")
        assert result["quality"] == "gold"


# ═══════════════════════════════════════════════════════════════════════════════
# validate_classification_labels
# ═══════════════════════════════════════════════════════════════════════════════


class TestValidateClassificationLabels:
    def test_valid_labels(self):
        labels = json.dumps({"cat": "A category", "dog": "Another one"})
        result = validate_classification_labels(labels)
        assert result == {"cat": "A category", "dog": "Another one"}

    def test_module_constant_is_valid(self):
        """The actual DOCUMENT_CLASSIFICATION_LABELS constant must pass validation."""
        result = validate_classification_labels(DOCUMENT_CLASSIFICATION_LABELS)
        assert len(result) == 5
        assert "payer_sheet" in result

    def test_invalid_json(self):
        with pytest.raises(ValueError, match="valid JSON"):
            validate_classification_labels("not json{")

    def test_not_a_dict(self):
        with pytest.raises(ValueError, match="JSON object"):
            validate_classification_labels('["a", "b"]')

    def test_too_few_labels(self):
        with pytest.raises(ValueError, match="at least 2"):
            validate_classification_labels(json.dumps({"only_one": "desc"}))

    def test_too_many_labels(self):
        labels = {f"label_{i}": f"desc {i}" for i in range(501)}
        with pytest.raises(ValueError, match="at most 500"):
            validate_classification_labels(json.dumps(labels))

    def test_label_name_too_long(self):
        labels = {"a" * 101: "desc", "b": "desc2"}
        with pytest.raises(ValueError, match="1-100 chars"):
            validate_classification_labels(json.dumps(labels))

    def test_description_too_long(self):
        labels = {"a": "x" * 1001, "b": "ok"}
        with pytest.raises(ValueError, match="<=1000 chars"):
            validate_classification_labels(json.dumps(labels))


# ═══════════════════════════════════════════════════════════════════════════════
# validate_extraction_schema
# ═══════════════════════════════════════════════════════════════════════════════


class TestValidateExtractionSchema:
    def test_simple_valid_schema(self):
        schema = json.dumps({"name": {"type": "string", "description": "A name"}})
        result = validate_extraction_schema(schema)
        assert "name" in result

    def test_module_constant_is_valid(self):
        """The actual DOCUMENT_EXTRACTION_SCHEMA constant must pass validation."""
        result = validate_extraction_schema(DOCUMENT_EXTRACTION_SCHEMA)
        assert "document_title" in result
        assert "segments" in result
        assert "fields" in result

    def test_invalid_json(self):
        with pytest.raises(ValueError, match="valid JSON"):
            validate_extraction_schema("{bad")

    def test_not_a_dict(self):
        with pytest.raises(ValueError, match="JSON object"):
            validate_extraction_schema('"just a string"')

    def test_forbidden_keyword_required(self):
        schema = json.dumps({"name": {"type": "string", "required": True}})
        with pytest.raises(ValueError, match="Unsupported keyword 'required'"):
            validate_extraction_schema(schema)

    def test_forbidden_keyword_anyOf(self):
        schema = json.dumps({"name": {"type": "string", "anyOf": []}})
        with pytest.raises(ValueError, match="Unsupported keyword 'anyOf'"):
            validate_extraction_schema(schema)

    def test_unsupported_type(self):
        schema = json.dumps({"name": {"type": "VARCHAR(100)"}})
        with pytest.raises(ValueError, match="Unsupported type"):
            validate_extraction_schema(schema)

    def test_enum_requires_labels(self):
        schema = json.dumps({"status": {"type": "enum"}})
        with pytest.raises(ValueError, match="requires non-empty 'labels'"):
            validate_extraction_schema(schema)

    def test_enum_empty_labels(self):
        schema = json.dumps({"status": {"type": "enum", "labels": []}})
        with pytest.raises(ValueError, match="requires non-empty 'labels'"):
            validate_extraction_schema(schema)

    def test_array_requires_items(self):
        schema = json.dumps({"tags": {"type": "array", "description": "Tags"}})
        with pytest.raises(ValueError, match="requires 'items'"):
            validate_extraction_schema(schema)

    def test_object_requires_properties(self):
        schema = json.dumps({"data": {"type": "object"}})
        with pytest.raises(ValueError, match="requires non-empty 'properties'"):
            validate_extraction_schema(schema)

    def test_max_depth_exceeded(self):
        """Build a schema 13 levels deep — should fail at depth 12."""
        inner = {"type": "string", "description": "leaf"}
        for _ in range(12):
            inner = {
                "type": "object",
                "properties": {"nested": inner},
            }
        schema = json.dumps({"root": inner})
        with pytest.raises(ValueError, match="max depth of 12"):
            validate_extraction_schema(schema)

    def test_max_properties_exceeded(self):
        """257 top-level properties should fail."""
        props = {f"field_{i}": {"type": "string"} for i in range(257)}
        schema = json.dumps(props)
        with pytest.raises(ValueError, match="max is 256"):
            validate_extraction_schema(schema)


# ═══════════════════════════════════════════════════════════════════════════════
# build_parse_document_expr
# ═══════════════════════════════════════════════════════════════════════════════


class TestBuildParseDocumentExpr:
    def test_contains_function_call(self):
        expr = build_parse_document_expr("/Volumes/cat/sch/vol/images")
        assert "ai_parse_document(" in expr
        assert "as parsed" in expr

    def test_contains_path(self):
        expr = build_parse_document_expr("/Volumes/cat/sch/vol/images")
        assert "/Volumes/cat/sch/vol/images" in expr

    def test_version_2(self):
        expr = build_parse_document_expr("/path")
        assert "'version', '2.0'" in expr

    def test_escapes_single_quotes(self):
        expr = build_parse_document_expr("/Volumes/it's/a/path")
        assert "it\\'s" in expr
        # The raw quote should NOT appear unescaped
        assert "it's" not in expr


# ═══════════════════════════════════════════════════════════════════════════════
# build_classify_expr
# ═══════════════════════════════════════════════════════════════════════════════


class TestBuildClassifyExpr:
    def test_contains_function_call(self):
        expr = build_classify_expr('{"a": "b", "c": "d"}', "instructions")
        assert "ai_classify(" in expr
        assert "as classification" in expr

    def test_version_2(self):
        expr = build_classify_expr('{"a": "b", "c": "d"}', "instr")
        assert "'version', '2.0'" in expr

    def test_escapes_single_quotes_in_labels(self):
        expr = build_classify_expr('{"it\'s": "a label"}', "instr")
        assert "it\\'s" in expr

    def test_escapes_single_quotes_in_instructions(self):
        expr = build_classify_expr('{"a": "b", "c": "d"}', "don't stop")
        assert "don\\'t" in expr

    def test_with_actual_constants(self):
        """Smoke test with real module constants."""
        expr = build_classify_expr(DOCUMENT_CLASSIFICATION_LABELS, "Classify docs.")
        assert "ai_classify(" in expr
        assert "payer_sheet" in expr


# ═══════════════════════════════════════════════════════════════════════════════
# build_extract_expr
# ═══════════════════════════════════════════════════════════════════════════════


class TestBuildExtractExpr:
    def test_contains_function_call(self):
        expr = build_extract_expr('{"x": {"type": "string"}}', "instr")
        assert "ai_extract(" in expr
        assert "as extracted" in expr

    def test_version_2(self):
        expr = build_extract_expr('{"x": {"type": "string"}}', "instr")
        assert "'version', '2.0'" in expr

    def test_escapes_single_quotes_in_schema(self):
        expr = build_extract_expr('{"it\'s": {"type": "string"}}', "instr")
        assert "it\\'s" in expr

    def test_with_actual_constants(self):
        """Smoke test with real module constants."""
        expr = build_extract_expr(DOCUMENT_EXTRACTION_SCHEMA, EXTRACTION_INSTRUCTIONS)
        assert "ai_extract(" in expr
        assert "document_title" in expr


# ═══════════════════════════════════════════════════════════════════════════════
# build_prep_search_explode_expr
# ═══════════════════════════════════════════════════════════════════════════════


class TestBuildPrepSearchExplodeExpr:
    def test_returns_tuple_of_two_lists(self):
        result = build_prep_search_explode_expr()
        assert isinstance(result, tuple)
        assert len(result) == 2
        stage_1, stage_2 = result
        assert isinstance(stage_1, list)
        assert isinstance(stage_2, list)

    def test_stage_1_has_prep_search(self):
        stage_1, _ = build_prep_search_explode_expr()
        assert any("ai_prep_search" in expr for expr in stage_1)

    def test_stage_2_has_inline(self):
        _, stage_2 = build_prep_search_explode_expr()
        assert any("inline(" in expr for expr in stage_2)

    def test_both_stages_carry_doc_source_id(self):
        stage_1, stage_2 = build_prep_search_explode_expr()
        assert "doc_source_id" in stage_1
        assert "doc_source_id" in stage_2

    def test_stage_2_chunk_fields(self):
        _, stage_2 = build_prep_search_explode_expr()
        combined = " ".join(stage_2)
        assert "chunk_id" in combined
        assert "chunk_position" in combined
        assert "chunk_to_retrieve" in combined
        assert "chunk_to_embed" in combined


# ═══════════════════════════════════════════════════════════════════════════════
# Constants integrity
# ═══════════════════════════════════════════════════════════════════════════════


class TestConstants:
    def test_default_table_properties_has_cdf(self):
        assert default_table_properties["delta.enableChangeDataFeed"] == "true"

    def test_default_table_properties_has_variant(self):
        assert default_table_properties["delta.feature.variantType-preview"] == "supported"

    def test_supported_extensions_has_pdf(self):
        assert ".pdf" in SUPPORTED_EXTENSIONS

    def test_supported_extensions_all_lowercase(self):
        for ext in SUPPORTED_EXTENSIONS:
            assert ext == ext.lower(), f"Extension {ext} should be lowercase"

    def test_supported_extensions_all_start_with_dot(self):
        for ext in SUPPORTED_EXTENSIONS:
            assert ext.startswith("."), f"Extension {ext} should start with '.'"

    def test_classification_labels_is_valid_json(self):
        labels = json.loads(DOCUMENT_CLASSIFICATION_LABELS)
        assert isinstance(labels, dict)
        assert len(labels) >= 2

    def test_extraction_schema_is_valid_json(self):
        schema = json.loads(DOCUMENT_EXTRACTION_SCHEMA)
        assert isinstance(schema, dict)
        assert "document_title" in schema
