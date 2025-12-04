from utilities.utils import DocumentParsing

DocumentParsingPipeline = DocumentParsing(
    spark = spark
    ,catalog = spark.conf.get("catalog_use")
    ,schema = spark.conf.get("schema_use")
    ,volume = spark.conf.get("volume_use")
    ,volume_sub_path = spark.conf.get("volume_sub_path_use")
)

    DocumentParsingPipeline.stream_ingest()