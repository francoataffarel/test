import os
import sys
import re
import string

from pyspark import SparkContext
from pyspark.sql import functions as f
from pyspark.sql.types import StringType

from awsglue import DynamicFrame
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from awsglue.context import GlueContext

glueContext = GlueContext(SparkContext.getOrCreate())

args = getResolvedOptions(sys.argv, ['GLUE_DB', 'GLUE_TABLE', 'GLUE_RDS_DB', 'GLUE_RDS_TABLE', 'JOB_NAME', 'ROLE_ARN', 'REDSHIFT_DB'])

GLUE_DB = args['GLUE_DB']
GLUE_TABLE = args['GLUE_TABLE']
GLUE_RDS_DB = args['GLUE_RDS_DB']
GLUE_RDS_TABLE = args['GLUE_RDS_TABLE']
ROLE_ARN = args['ROLE_ARN']
REDSHIFT_DB = args['REDSHIFT_DB']

job = Job(glueContext)
job.init(args['JOB_NAME'], args)
print("STARTED")


#########################################
### Extract (Read)
#########################################

input_df = glueContext.create_dynamic_frame.from_catalog(
    database=GLUE_DB,
    table_name=GLUE_TABLE,
    transformation_ctx="input_df")

input_df = input_df.toDF()

# If there is no data in input_df, exit the job with status success
if input_df.count() == 0:
    print("No data. Exiting")
    job.commit()
    os._exit(os.EX_OK)

print(f"Reading data from {GLUE_TABLE}")
print(f"Size of input_df = {input_df.count()}")
input_df.show(5)

input_rds_df = glueContext.create_dynamic_frame.from_catalog(database=GLUE_RDS_DB, table_name=GLUE_RDS_TABLE)
input_rds_df = input_rds_df.toDF()

print(f"Reading data from {GLUE_RDS_TABLE}")
print(f"Size of input_rds_df = {input_rds_df.count()}")
input_rds_df.show(5)

#########################################
### TRANSFORM (Modify)
#########################################

input_df = input_df.withColumn('extracted_date', f.to_date('extracted_date', 'yyyyMMdd'))


input_rds_df = input_rds_df.select('brand', 'brand_type')
input_rds_df = input_rds_df.dropDuplicates(['brand'])
input_rds_df.show(5)

input_df = input_df.join(input_rds_df, "brand", "LEFT")
input_df = input_df.fillna('OTHER', subset=['brand_type'])

input_df = input_df.select('brand', 'channel', 'name', 'skuid', 'product_href', 'mrp', 'price', 'discount', 'rating',
                           'rating_count', 'rank', 'page', 'sponsored', 'rank_sponsored', 'extracted_date',
                           'primary_page_name', 'primary_scroll_times', 'primary_nav', 'primary_nav_type',
                           'primary_nav_title', 'primary_nav_subtitle', 'primary_nav_url', 'primary_nav_has_brand_name',
                           'secondary_page_name', 'secondary_scroll_times', 'secondary_nav', 'secondary_nav_type',
                           'secondary_nav_title', 'secondary_nav_subtitle', 'secondary_nav_url',
                           'secondary_nav_has_brand_name', 'offer_type', 'offer_title', 'offer_start_date',
                           'offer_end_date', 'brand_type')

print(f"Size of product_table = {input_df.count()}")
input_df.show(5)

#########################################
### Write to Redshift Table (Write)
#########################################

glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=DynamicFrame.fromDF(input_df, glueContext, 'product'),
    catalog_connection=REDSHIFT_DB,
    connection_options={
        "dbtable": "brand_analytics.browse_product",
        "database": REDSHIFT_DB,
        "aws_iam_role": ROLE_ARN
    },
    redshift_tmp_dir=args["TempDir"]
)

print("COMPLETED")
job.commit()
