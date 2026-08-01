from pyspark import pipelines as dp

import yaml

# Path to the YAML file (adjust if needed for your environment)
yaml_path = '../../../fixtures/config/specification_further_processing/temporary_view_control.yml'

# Read the YAML file
with open(yaml_path, 'r') as file:
    content = file.read()

# Parse YAML to Python dictionary
temporary_views_dict = yaml.safe_load(content)

from pyspark import pipelines as dp

@dp.temporary_view(
	name="tv_source"
	, comment="Temporary view over specification_documents_parsed table"
)
def specification_documents_parsed_temp():
	return (
		spark.read.table("ncpdp_dev.dev_matthew_giglia_rx_claims.specification_documents_parsed")
	)
 
def further_processing_tables(name, sql):
    @dp.table(
        name=name
    )
    def temp_view():
        return (
            spark.sql(sql)
        )

for tv in temporary_views_dict['document_parsing_sql']:
    further_processing_tables(tv['name'], tv['sql'])

 
 
 
