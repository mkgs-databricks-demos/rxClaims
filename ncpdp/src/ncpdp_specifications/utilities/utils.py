"""
The 'utilities' folder contains Python modules.
Keeping them separate provides a clear overview
of utilities you can reuse across your transformations.
"""
from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType
    ,StructField
    ,StringType
    ,BinaryType
    ,IntegerType
    ,LongType
    ,TimestampType
    ,FloatType
    ,BooleanType
)
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import (
    col
    ,current_timestamp
    ,lit
    ,udf
    ,sha2
    ,concat_ws
    ,from_xml
    ,array_contains
    ,ai_parse_document
    ,lower
    ,regexp_extract
)
from typing import Any

default_table_properties = {
  'delta.enableChangeDataFeed' : 'true'
  ,'delta.enableDeletionVectors' : 'true'
  ,'delta.enableRowTracking' : 'true'
  ,'delta.autoOptimize.optimizeWrite' : 'true' 
  ,'delta.autoOptimize.autoCompact' : 'true'
  ,'delta.feature.variantType-preview' : 'supported'
  ,'delta.enableVariantShredding' : 'true'
#   ,'delta.feature.catalogOwned-preview' : 'supported'
}

class DocumentParsing:
    def __init__(self, spark: SparkSession, catalog: str, schema: str, volume: str, volume_sub_path: str = None, table_properties: dict = default_table_properties):
        self.spark = spark
        self.catalog = catalog 
        self.schema = schema
        self.volume = volume
        self.volume_sub_path = volume_sub_path
        self.table_properties = table_properties.copy()
        self.table_properties['quality'] = 'bronze'

    def __repr__(self):
        return f"DocumentParsing(catalog='{self.catalog}', schema='{self.schema}', volume='{self.volume}',volume_sub_path='{self.volume_sub_path}')"
      
    def stream_ingest(self):
        schema_definition = f"""
            spec_file_source_id STRING NOT NULL PRIMARY KEY COMMENT 'Unique identifier for the ingested file.',
            file_metadata STRUCT < file_path: STRING, 
            file_name: STRING,
            file_size: BIGINT,
            file_block_start: BIGINT,
            file_block_length: BIGINT,
            file_modification_time: TIMESTAMP > NOT NULL COMMENT 'Metadata about the file ingested.'
            ,ingest_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP() COMMENT 'The date timestamp the file was ingested.'
            ,path STRING COMMENT 'The path to the file starting with dbfs:/'
            ,modificationTime TIMESTAMP COMMENT 'The date timestamp the file was modified.'
            ,length LONG COMMENT 'Length of the file in bytes.'
            ,content BINARY COMMENT 'Binary representation of the file.'
        """

        if self.volume_sub_path == None:
            volume_path = f"/Volumes/{self.catalog}/{self.schema}/{self.volume}"
        else:
            volume_path = f"/Volumes/{self.catalog}/{self.schema}/{self.volume}/{self.volume_sub_path}"

        @dp.table(
            name=f"{self.catalog}.{self.schema}.specification_documents",
            comment=f"Streaming ingestion of NCPDP Specification Documents.",
            # spark_conf={"<key>" : "<value>", "<key>" : "<value>"},
            table_properties=self.table_properties,
            # path="<storage-location-path>",
            # partition_cols=["<partition-column>", "<partition-column>"],
            cluster_by = ["path"],
            cluster_by_auto=True,
            schema=schema_definition,
            # row_filter = "row-filter-clause",
            temporary=False
        )
        # @dp.expect(...)
        def stream_ingest_function():
            return (
                self.spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "binaryFile")
                .load(volume_path)
                .selectExpr("sha2(concat(_metadata.*), 256) as spec_file_source_id", "_metadata as file_metadata", "*")
            )

    def parse_documents(self): 
        @dp.temporary_view(
            name = "parsed_documents"
        )
        def parse_documents_function():
            return(
                self.spark.readStream
                .table(f"{self.catalog}.{self.schema}.specification_documents")
                .filter(
                    array_contains(
                        lit(['.pdf', '.jpg', '.jpeg', '.png', '.doc', '.docx', '.ppt', '.pptx'])
                        ,lower(regexp_extract(col('path'), r'(\.[^.]+)$', 1))
                    )
		        )
                .selectExpr("spec_file_source_id", "path", "ai_parse_document(content) as parsed")
            )
        
        @dp.temporary_view(
            name = "raw_documents"
        )
        def raw_documents_function():
            return(
                self.spark.readStream
                .table(f"{self.catalog}.{self.schema}.specification_documents")
                .filter(
                    array_contains(
                        lit(['.pdf', '.jpg', '.jpeg', '.png', '.doc', '.docx', '.ppt', '.pptx'])
                        ,lower(regexp_extract(col('path'), r'(\.[^.]+)$', 1))
                    ) == False
                )
                .selectExpr("spec_file_source_id", "path", "null as raw_parsed", "decode(content, 'utf-8') as text", "null as error_status","ai_parse_document(content) as parsed")
            )
        

