from utilities.utils import DocumentParsing

DocumentParsingPipeline = DocumentParsing(
    spark = spark
    ,catalog = spark.conf.get("catalog_use")
    ,schema = spark.conf.get("schema_use")
    ,volume = spark.conf.get("volume_use")
    ,volume_sub_path = spark.conf.get("volume_sub_path_use")
    ,volume_parsed_image_output_sub_path = spark.conf.get("volume_parsed_image_output_sub_path_use")
)

DocumentParsingPipeline.stream_ingest()
# waiting for the Spark Declarative Pipeline Runtime to be at least 17.3 before continuing development and running the parse_documents class method.  as of 20260106 the current and preview channels are both 16.4.
# DocumentParsingPipeline.parse_documents()