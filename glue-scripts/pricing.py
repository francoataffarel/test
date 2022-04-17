import os
import sys
from pyspark import SparkContext
from pyspark.sql import functions as f
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from awsglue.context import GlueContext
from awsglue import DynamicFrame
from pyspark.sql.types import DoubleType

glueContext = GlueContext(SparkContext.getOrCreate())

args = getResolvedOptions(sys.argv, ['GLUE_DB', 'GLUE_PRICING_TABLE', 'JOB_NAME', 'ROLE_ARN', 'REDSHIFT_DB', 'GLUE_RDS_DB', 'GLUE_RDS_TABLE'])

GLUE_DB = args['GLUE_DB']
GLUE_PRICING_TABLE = args['GLUE_PRICING_TABLE']
ROLE_ARN = args['ROLE_ARN']
REDSHIFT_DB = args['REDSHIFT_DB']
REGION_NAME = 'ap-south-1'
GLUE_RDS_DB = args['GLUE_RDS_DB']
GLUE_RDS_TABLE = args['GLUE_RDS_TABLE']

job = Job(glueContext)
job.init(args['JOB_NAME'], args)
print("STARTED")

#########################################
### Extract (Read)
#########################################

input_df = glueContext.create_dynamic_frame.from_catalog(
    database=GLUE_DB,
    table_name=GLUE_PRICING_TABLE,
    transformation_ctx="input_df")

input_df = input_df.toDF()
# If there is no data in input_df, exit the job with status success
if input_df.count() == 0:
    print("No data. Exiting")
    job.commit()
    os._exit(os.EX_OK)

print(f"Reading data from {GLUE_PRICING_TABLE}")
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
input_df = input_df.withColumn("mrp", input_df["mrp"].cast(DoubleType()))
input_df = input_df.withColumn("price", input_df["price"].cast(DoubleType()))
input_df = input_df.withColumn("discount", input_df["discount"].cast(DoubleType()))
input_df = input_df.withColumn("discount_percentage", input_df["discount_percentage"].cast(DoubleType()))
input_df = input_df.dropDuplicates(['skuid'])

input_rds_df = input_rds_df.select('brand', 'brand_type')
input_rds_df = input_rds_df.dropDuplicates(['brand'])
input_rds_df.show(5)

input_df = input_df.join(input_rds_df, "brand", "LEFT")
input_df = input_df.fillna('OTHER', subset=['brand_type'])

#########################################
### Prepare Tables
#########################################

product_table = input_df.select('skuid', 'brand', 'category', 'title', 'product_href',
                                'mrp', 'price', 'discount', 'discount_percentage', 'extracted_date', 'bank_offers',
                                'coupon_offers', 'available', 'marketplace', 'brand_type')                                

print(f"Size of review_table = {product_table.count()}")
product_table.show(5)

#########################################
### Write to Redshift
#########################################
print("Writing to Redshift brand_analytics.pricing_product table")
glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=DynamicFrame.fromDF(product_table, glueContext, 'product'),
    catalog_connection=REDSHIFT_DB,
    connection_options={
        "dbtable": "brand_analytics.pricing_product",
        "database": REDSHIFT_DB,
        "aws_iam_role": ROLE_ARN
    },
    redshift_tmp_dir=args["TempDir"]
)

print("COMPLETED")
job.commit()
