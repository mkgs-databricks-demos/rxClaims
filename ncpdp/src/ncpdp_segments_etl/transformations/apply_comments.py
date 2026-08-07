# This file has been intentionally emptied.
#
# Column comments are now applied declaratively via the schema= parameter
# in @dp.table() — see segments.py and SegmentBuilder.build_schema_ddl().
#
# The previous approach (ALTER TABLE ALTER COLUMN COMMENT via spark.sql())
# is not supported inside SDP pipelines.
#
# The claimbilling_silver_column_comments_log table is no longer produced.
# Column comments are part of the table definition itself.
#
# This file is kept empty to avoid import errors from the pipeline's
# glob include pattern. It will be removed in a future cleanup.
