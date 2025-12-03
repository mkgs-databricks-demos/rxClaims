from pyspark import pipelines as dp
from pyspark.sql.functions import col

from utilities.utils import Bronze

file_types = spark.conf.get("file_types").split(',')
file_types = [file_type.strip() for file_type in file_types]

for file_type in file_types:
    BronzePipeline = Bronze(
        spark = spark
        ,catalog = spark.conf.get("catalog_use")
        ,schema = spark.conf.get("schema_use")
        ,volume = spark.conf.get("volume_use")
        ,volume_sub_path = f"{spark.conf.get('volume_sub_path_use'}/{file_type})"
    )

    BronzePipeline.stream_ingest()
    BronzePipeline.variant_transform()
    BronzePipeline.extract_requests()
    BronzePipeline.extract_responses()
    BronzePipeline.extract_supplemental()

