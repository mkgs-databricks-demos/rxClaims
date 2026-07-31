"""NCPDP Document Intelligence Pipeline - Transformation Entry Point

Orchestrates the full document intelligence chain:
  1. stream_ingest: Auto Loader binary ingestion from UC Volume
  2. parse_documents: ai_parse_document v2 with image output + figure descriptions
  3. classify_documents: ai_classify v2 with descriptive NCPDP document type labels
  4. extract_fields: ai_extract v2 with typed NCPDP segment/field schema
  5. prep_search: ai_prep_search semantic chunking for vector search / RAG
"""
from utilities.utils import DocumentIntelligence

# Instantiate the pipeline from spark configuration variables
pipeline = DocumentIntelligence(
    spark=spark,
    catalog=spark.conf.get("catalog_use"),
    schema=spark.conf.get("schema_use"),
    volume=spark.conf.get("volume_use"),
    volume_sub_path=spark.conf.get("volume_sub_path_use", None) or None,
    image_output_sub_path=spark.conf.get("image_output_sub_path_use", None) or None,
)

# Execute the full document intelligence chain
pipeline.stream_ingest()
pipeline.parse_documents()
pipeline.classify_documents()
pipeline.extract_fields()
pipeline.prep_search()
