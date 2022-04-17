import sys
import os
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

args = getResolvedOptions(sys.argv, ['GLUE_DB', 'DEST_BUCKET', 'GLUE_CATEGORY_TABLE', 'GLUE_SKU_TABLE', 'JOB_NAME'])
GLUE_DB = args['GLUE_DB']
GLUE_CATEGORY_TABLE = args['GLUE_CATEGORY_TABLE']
GLUE_SKU_TABLE = args['GLUE_SKU_TABLE']
DEST_BUCKET = args['DEST_BUCKET']

DEFAULT_CATEGORY = 'NA'
DEFAULT_SUBCATEGORY = 'NA'

job = Job(glueContext)
job.init(args['JOB_NAME'], args)
print("STARTED")

# list of columns required in cleaned category table data
CATEGORY_TBL_COLUMNS_REQUIRED = ['keyword', 'category', 'subcategory']

# list of columns required in cleaned data
SKU_TBL_COLUMNS_REQUIRED = ['brand_id', 'brand', 'category', 'subcategory', 'skuid', 'amazon_skuid', 'flipkart_skuid',
                            'myntra_skuid', 'nykaa_skuid', 'ajio_skuid']


# Helper Clean Text Function
def clean_text(text):
    if text is None:
        return None
    text = re.sub(r'[%s]' % re.escape(string.punctuation), '', text.lower())
    text = re.sub(r'\w*\d\w*', '', text)
    re_emoji = re.compile(r'[\U00010000-\U0010ffff]', flags=re.UNICODE)
    text = re_emoji.sub(r'', text)
    text = re.sub(r'[^a-z] ', '', text)
    return text


#########################################
### User Defined Functions
#########################################

get_clean_udf = f.udf(lambda l: clean_text(l), StringType())

#########################################
### Extract Category & SKU Data (Read)
#########################################


input_cat_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=GLUE_DB,
    table_name=GLUE_CATEGORY_TABLE,
    transformation_ctx="input_cat_df")

input_cat_df = input_cat_dyf.toDF()

input_sku_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=GLUE_DB,
    table_name=GLUE_SKU_TABLE,
    transformation_ctx="input_sku_df")

input_sku_df = input_sku_dyf.toDF()

# If there is no data in input_cat_df, exit the job with status success
if input_cat_df.count() == 0 and input_sku_df.count() == 0:
    print("No data. Exiting")
    job.commit()
    os._exit(os.EX_OK)

if input_cat_df.count() != 0:
    input_cat_df = input_cat_df.select('keyword', 'category', 'subcategory')

    print(f"Reading data from {GLUE_CATEGORY_TABLE}")
    print(f"Size of input_cat_df = {input_cat_df.count()}")
    input_cat_df.show(5)

if input_sku_df.count() != 0:
    input_sku_df = input_sku_df.select('brand', 'skuid', 'category', 'subcategory', 'amazon_skuid', 'flipkart_skuid',
                                       'myntra_skuid', 'nykaa_skuid', 'ajio_skuid')

    print(f"Reading data from {GLUE_SKU_TABLE}")
    print(f"Size of input_sku_df = {input_sku_df.count()}")
    input_sku_df.show(5)


#########################################
### TRANSFORM CATEGORY TABLE (Modify)
#########################################

if input_cat_df.count() != 0:

    input_cat_df = input_cat_df.replace('nan', '').na.fill('')
    input_cat_df = input_cat_df.withColumn('keyword', get_clean_udf(f.col('keyword')))
    input_cat_df = input_cat_df.filter(f.col("keyword") != '')

    print("Alert for Data With Null Values")
    alert_cat_df = input_cat_df.filter((f.col('keyword').isNull())
                                       | (f.col('category').isNull())
                                       | (f.col('subcategory').isNull())
                                       | (f.col('keyword') == '')
                                       | (f.col('category') == '')
                                       | (f.col('subcategory') == '')
                                       )

    # TODO: Send alert for these Null Values
    alert_cat_df.show(50)

    print("Keep only data with Non-Null fields in a few mandatory columns")
    input_cat_df = input_cat_df.filter((f.col('keyword').isNotNull())
                                       & (f.col('category').isNotNull())
                                       & (f.col('subcategory').isNotNull())
                                       & (f.col('keyword') != '')
                                       & (f.col('category') != '')
                                       & (f.col('subcategory') != '')
                                       )


    # TODO: Send alert for these Null Values

    # Rearranging the required columns and dropping others
    input_cat_df = input_cat_df.select(CATEGORY_TBL_COLUMNS_REQUIRED)

    print("Remove duplicated based on keyword")
    input_cat_df = input_cat_df.coalesce(1).dropDuplicates(subset=['keyword'])

    print(f"After cleaning data, size of input_cat_df = {input_cat_df.count()}")
    input_cat_df.show(5)

    # Throw error if no data found after cleaning
    # if input_cat_df.count() == 0:
    #     sys.exit(1)

#########################################
### TRANSFORM SKU TABLE (Modify)
#########################################

if input_sku_df.count() != 0:

    input_sku_df = input_sku_df.replace('nan', '').na.fill('')
    input_sku_df = input_sku_df\
        .withColumn('brand_id_without_spc', f.regexp_replace(f.col('brand'), '[^A-Za-z0-9]', '')) \
        .withColumn('brand_id_without_space', f.regexp_replace(f.col('brand_id_without_spc'), '\s+', '_')) \
        .withColumn('brand_id', f.lower(f.col('brand_id_without_space')))

    print("Alert for Data With Null Values")
    alert_sku_df = input_sku_df.filter((f.col('brand_id').isNull())
                                       | (f.col('skuid').isNull())
                                       | (f.col('category').isNull())
                                       | (f.col('subcategory').isNull())
                                       | (f.col('brand_id') == '')
                                       | (f.col('skuid') == '')
                                       | (f.col('category') == '')
                                       | (f.col('subcategory') == '')
                                       )

    alert_sku_df.show(50)

    # TODO: Send alert for these Null Values

    print("Keep only data with Non-Null fields in a few mandatory columns")
    input_sku_df = input_sku_df.filter((f.col('brand_id').isNotNull())
                                       & (f.col('skuid').isNotNull())
                                       & (f.col('category').isNotNull())
                                       & (f.col('subcategory').isNotNull())
                                       & (f.col('brand_id') != '')
                                       & (f.col('skuid') != '')
                                       & (f.col('category') != '')
                                       & (f.col('subcategory') != '')
                                       )

    # Rearranging the required columns and dropping others
    input_sku_df = input_sku_df.select(SKU_TBL_COLUMNS_REQUIRED)

    print("Remove duplicated based on skuid")
    input_sku_df = input_sku_df.coalesce(1).dropDuplicates(subset=['skuid'])

    print(f"After cleaning data, size of input_sku_df = {input_sku_df.count()}")
    input_sku_df.show(5)

    # Throw error if no data found after cleaning
    # if input_sku_df.count() == 0:
    #     sys.exit(1)


#########################################
# Throw error if no data found after cleaning
#########################################

if input_cat_df.count() == 0 and input_sku_df.count() == 0:
    sys.exit(1)

#########################################
### Load Category Table (Write)
#########################################

if input_cat_df.count() != 0:
    print("writing data to s3 for category_table ")
    glueContext.write_dynamic_frame.from_options(
        frame=DynamicFrame.fromDF(input_cat_df, glueContext, 'category_table'),
        connection_type="s3",
        format="parquet",
        connection_options={
            "path": "s3://" + DEST_BUCKET + "/category/",
            "compression": "gzip"
        }
    )

#########################################
### Load SKU Table (Write)
#########################################

if input_sku_df.count() != 0:
    print("writing data to s3 for product_table ")
    glueContext.write_dynamic_frame.from_options(
        frame=DynamicFrame.fromDF(input_sku_df, glueContext, 'product_table'),
        connection_type="s3",
        format="parquet",
        connection_options={
            "path": "s3://" + DEST_BUCKET + "/product/",
            "compression": "gzip"
        }
    )


print("COMPLETED")
job.commit()
