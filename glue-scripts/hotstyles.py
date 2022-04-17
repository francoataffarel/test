import os
import sys

from awsglue import DynamicFrame
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark import SparkContext
from pyspark.sql import functions as f


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

input_df = glueContext.create_dynamic_frame.from_catalog(database=GLUE_DB, table_name=GLUE_TABLE,
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

input_df = input_df.withColumn('extracted_date', f.to_date('extracted_date', 'yyyy-MM-dd'))
input_df = input_df.withColumnRenamed('skuid', 'channel_skuid')

input_rds_df = input_rds_df.select('skuid', 'channel_skuid', 'channel_parent_skuid', 'brand', 'category',
                                   'details', 'style_type', 'added_date', 'expiry_date')
input_rds_df = input_rds_df.dropDuplicates(['channel_skuid'])
print(f"input_rds_df after dropping duplicates = {input_df.count()}")
input_rds_df.show(5)

input_df = input_df.join(input_rds_df, "channel_skuid", "LEFT")
print(f"input_df after join = {input_df.count()}")
input_df.show(5)

#########################################
### Prepare Tables
#########################################

product_table = input_df.select('skuid', 'channel_skuid', 'channel_parent_skuid', 'brand', 'channel', 'category',
                                'details', 'style_type', 'added_date', 'expiry_date', 'name', 'product_href',
                                'mrp', 'price', 'discount', 'rating', 'rating_count', 'category_1', 'category_2',
                                'category_3', 'category_4', 'rank_1', 'rank_2', 'rank_3', 'rank_4', 'extracted_date')

print(f"Size of product_table = {product_table.count()}")
product_table.show(5)

#########################################
### Write to Redshift
#########################################
print("Writing to Redshift brand_analytics.hotstyles_product table")
glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=DynamicFrame.fromDF(product_table, glueContext, 'product'),
    catalog_connection=REDSHIFT_DB,
    connection_options={
        "dbtable": "brand_analytics.hotstyles_product",
        "database": REDSHIFT_DB,
        "aws_iam_role": ROLE_ARN
    },
    redshift_tmp_dir=args["TempDir"]
)

print("COMPLETED")
job.commit()
