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
  # ,'delta.feature.catalogOwned-preview' : 'supported'
}

class Bronze:
    def __init__(self, spark: SparkSession, catalog: str, schema: str, volume: str, file_type: str, volume_sub_path: str = None, table_properties: dict = default_table_properties):
        self.spark = spark
        self.catalog = catalog 
        self.schema = schema
        self.volume = volume
        self.file_type = file_type
        self.volume_sub_path = volume_sub_path
        self.table_properties = table_properties.copy()
        self.table_properties['quality'] = 'bronze'

    def __repr__(self):
        return f"Bronze(catalog='{self.catalog}', schema='{self.schema}', volume='{self.volume}',volume_sub_path='{self.volume_sub_path}')"
      
    def stream_ingest(self):
      schema_definition = f"""
        transaction_file_source_id STRING NOT NULL PRIMARY KEY COMMENT 'Unique identifier for the ingested file.',
        file_metadata STRUCT < file_path: STRING, 
        file_name: STRING,
        file_size: BIGINT,
        file_block_start: BIGINT,
        file_block_length: BIGINT,
        file_modification_time: TIMESTAMP > NOT NULL COMMENT 'Metadata about the file ingested.'
        ,ingest_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP() COMMENT 'The date timestamp the file was ingested.'
        ,value STRING COMMENT 'The raw XML file contents.'
      """

      if self.volume_sub_path == None:
        volume_path = f"/Volumes/{self.catalog}/{self.schema}/{self.volume}/{self.file_type}"
      else:
        volume_path = f"/Volumes/{self.catalog}/{self.schema}/{self.volume}/{self.volume_sub_path}/{self.file_type}"

      @dp.table(
        name=f"{self.catalog}.{self.schema}.{self.file_type}_bronze",
        comment=f"Streaming bronze ingestion of NCPDP {self.file_type} XML files as full text strings.",
        # spark_conf={"<key>" : "<value>", "<key>" : "<value>"},
        table_properties=self.table_properties,
        # path="<storage-location-path>",
        # partition_cols=["<partition-column>", "<partition-column>"],
        cluster_by = ["file_metadata.file_path"],
        cluster_by_auto=True,
        schema=schema_definition,
        # row_filter = "row-filter-clause",
        temporary=False
      )
      # @dp.expect(...)
      def stream_ingest_function():
          return (self.spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "text")
            .load(volume_path)
            .selectExpr("sha2(concat(_metadata.*), 256) as transaction_file_source_id", "_metadata as file_metadata", "*")
          )

    def variant_transform(self):
      @dp.table(
        name = f"{self.catalog}.{self.schema}.{self.file_type}_bronze_variant"
        ,comment = f"Streaming bronze transformation from NCPDP {self.file_type} XML files as full text strings to variant."
        ,table_properties=self.table_properties
        ,cluster_by_auto=True
      )
      def variant_transform_function():
        return (self.spark.readStream
          .table(f"{self.catalog}.{self.schema}.{self.file_type}_bronze")
          .withColumn("messages", from_xml(col("value"), "VARIANT"))
          .drop(col("value"))
        )

    def extract_requests(self):
      @dp.table(
        name = f"{self.catalog}.{self.schema}.{self.file_type}_bronze_requests"
        ,comment = f"Initial parsing of the NCPDP {self.file_type} Request Messages"
        ,table_properties=self.table_properties
        ,cluster_by_auto=True
      )
      def extract_requests_function():
        return (
          self.spark.sql(f"""
            SELECT
              transaction_file_source_id
              ,messages
              ,requests.pos as request_pos
              ,requests.key as request_segment
              ,request_value.*
            FROM 
              STREAM ({self.catalog}.{self.schema}.{self.file_type}_bronze_variant)
              ,LATERAL variant_explode_outer(messages:NcpdpRequest) as requests
              ,LATERAL variant_explode_outer(requests.value) as request_value
          """)
        )

    def extract_responses(self):
      @dp.table(
        name = f"{self.catalog}.{self.schema}.{self.file_type}_bronze_responses"
        ,comment = f"Initial parsing of the NCPDP {self.file_type} Response Messages"
        ,table_properties=self.table_properties
        ,cluster_by_auto=True
      )
      def extract_responses_function():
        return (
          self.spark.sql(f"""
            SELECT
              transaction_file_source_id
              ,messages
              ,responses.pos as response_pos
              ,responses.key as response_segment
              ,response_value.*
            FROM 
              STREAM ({self.catalog}.{self.schema}.{self.file_type}_bronze_variant)
              ,LATERAL variant_explode_outer(messages:NcpdpResponse) as responses
              ,LATERAL variant_explode_outer(responses.value) as response_value
          """)
        )

    def extract_supplemental(self):
      @dp.table(
        name = f"{self.catalog}.{self.schema}.{self.file_type}_bronze_supplemental"
        ,comment = f"Initial parsing of the NCPDP {self.file_type} Supplemental Messages"
        ,table_properties=self.table_properties
        ,cluster_by_auto=True
      )
      def extract_supplemental_function():
        return (
          self.spark.sql(f"""
            SELECT
              transaction_file_source_id
              ,messages
              ,supplemental.*
            FROM 
              STREAM ({self.catalog}.{self.schema}.{self.file_type}_bronze_variant)
              ,LATERAL variant_explode_outer(messages:Supplemental) as supplemental
          """)
        )
    
    ###################################
    # other class methods
    ###################################

    def to_dict(self):
        return {"spark": self.spark, "catalog": self.catalog, "schema": self.schema, "volume_sub_path": self.volume_sub_path}

    @classmethod
    def from_dict(cls, data):
        return cls(data['spark'], data['catalog'], data['schema'], data['volume_sub_path'])

    

