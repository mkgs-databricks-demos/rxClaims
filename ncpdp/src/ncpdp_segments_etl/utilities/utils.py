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

class Segments:
    def __init__(self, spark: SparkSession, catalog: str, schema: str,  file_type: str, segment_rules : dict, table_properties: dict = default_table_properties):
        self.spark = spark
        self.catalog = catalog 
        self.schema = schema
        self.file_type = file_type
        self.table_properties = table_properties.copy()
        self.segment_rules = segment_rules
        self.table_properties['quality'] = 'bronze'
        self.view_types = ["requests", "responses", "supplemental"]

    def set_up_source_views(self):
        @dp.view(
            name = f"v_{self.file_type}_bronze_requests"
        )
        def bronze_view():
            return self.spark.readStream.table(f"{self.catalog}.{self.schema}.{self.file_type}_bronze_requests")
        
        @dp.view(
            name = f"v_{self.file_type}_bronze_responses"
        )
        def bronze_view():
            return self.spark.readStream.table(f"{self.catalog}.{self.schema}.{self.file_type}_bronze_responses")
        
        @dp.view(
            name = f"v_{self.file_type}_bronze_supplemental"
        )
        def bronze_view():
            return self.spark.readStream.table(f"{self.catalog}.{self.schema}.{self.file_type}_bronze_supplemental")
            
    def review_segments(self):
        @dp.table(
            name = f"{self.catalog}.{self.schema}.{self.file_type}_request_segments"
            ,table_properties=self.table_properties
            ,cluster_by_auto=True
        )
        def segments():
            return self.spark.readStream.table(f"v_{self.file_type}_bronze_requests")
