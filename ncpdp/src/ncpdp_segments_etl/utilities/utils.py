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
    def __init__(self, spark: SparkSession, catalog: str, schema: str,  file_type: str, table_properties: dict = default_table_properties):
        self.spark = spark
        self.catalog = catalog 
        self.schema = schema
        self.file_type = file_type
        self.table_properties = table_properties.copy()
        self.table_properties['quality'] = 'bronze'

    def set_up_source_views(self):
        