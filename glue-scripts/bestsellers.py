import os
import sys

import boto3
from awsglue import DynamicFrame
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark import SparkContext
from pyspark.sql import functions as f


glueContext = GlueContext(SparkContext.getOrCreate())
glueClient = boto3.client('glue', region_name='ap-south-1')

args = getResolvedOptions(sys.argv, ['GLUE_DB', 'GLUE_TABLE', 'JOB_NAME', 'ROLE_ARN', 'REDSHIFT_DB'])

GLUE_DB = args['GLUE_DB']
GLUE_TABLE = args['GLUE_TABLE']
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


#########################################
### TRANSFORM (Modify)
#########################################

input_df = input_df.withColumn('extracted_date', f.to_date('extracted_date', 'yyyyMMdd'))

#########################################
### Prepare Tables
#########################################

product_table = input_df.select('skuid', 'brand', 'channel', 'name', 'product_href', 'category_1', 'category_2',
                                'category_3', 'category_4', 'rank_1', 'rank_2', 'rank_3', 'rank_4', 'extracted_date')

print(f"Size of product_table = {product_table.count()}")
product_table.show(5)

#########################################
### Write to Redshift
#########################################
print("Writing to Redshift brand_analytics.bestsellers_product table")
glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=DynamicFrame.fromDF(product_table, glueContext, 'product'),
    catalog_connection=REDSHIFT_DB,
    connection_options={
        "dbtable": "brand_analytics.bestsellers_product",
        "database": REDSHIFT_DB,
        "aws_iam_role": ROLE_ARN
    },
    redshift_tmp_dir=args["TempDir"]
)

print("COMPLETED")
job.commit()


