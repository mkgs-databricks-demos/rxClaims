from utilities.utils import Segments
import yaml
import glob

file_types = spark.conf.get("file_types").split(',')
file_types = [file_type.strip() for file_type in file_types]

segments_yaml_path = '../../../fixtures/config/segments/"
yml_files = glob.glob(f"{segments_yaml_path}*.yml")

segment_rules = []
for yml_file in yml_files:
    # Read the YAML file
    with open(yml_file, 'r') as file:
        content = file.read()
    # Parse YAML to Python dictionary
    segment_rules.append(yaml.safe_load(content))

for file_type in file_types:
    SegmentsPipeline = Segments(
        spark = spark
        ,catalog = spark.conf.get("catalog_use")
        ,schema = spark.conf.get("schema_use")
        ,file_type = file_type
        ,segment_rules = [r for r in segment_rules if r["type"] == file_type][0]
    )

    SegmentsPipeline.set_up_source_views()
    SegmentsPipeline.review_segments()

