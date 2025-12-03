from pyspark import pipelines as dp
from pyspark.sql.functions import col

from utilities.utils import Bronze

BronzePipeline = Bronze(
    spark = spark
    ,catalog = spark.conf.get("catalog_use")
    ,schema = spark.conf.get("schema_use")
    ,volume = spark.conf.get("volume_use")
    ,volume_sub_path = spark.conf.get("volume_sub_path_use")
)

BronzePipeline.stream_ingest()
BronzePipeline.variant_transform()
BronzePipeline.extract_requests()

