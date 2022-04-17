import os
import re
import string
import sys

import boto3
from awsglue import DynamicFrame
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark import SparkContext
from pyspark.shell import spark
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, MapType

number_pattern = re.compile("^[0-9]+$")

glueContext = GlueContext(SparkContext.getOrCreate())
glueClient = boto3.client('glue', region_name='ap-south-1')

args = getResolvedOptions(sys.argv, ['GLUE_DB', 'GLUE_TABLE', 'JOB_NAME', 'PRODUCT_GLUE_TABLE',
                                     'CATEGORY_GLUE_TABLE', 'MAPPING_GLUE_DB', 'ROLE_ARN', 'REDSHIFT_DB'])
GLUE_DB = args['GLUE_DB']
GLUE_TABLE = args['GLUE_TABLE']
MAPPING_GLUE_DB = args['MAPPING_GLUE_DB']
PRODUCT_GLUE_TABLE = args['PRODUCT_GLUE_TABLE']
CATEGORY_GLUE_TABLE = args['CATEGORY_GLUE_TABLE']
ROLE_ARN = args['ROLE_ARN']
REDSHIFT_DB = args['REDSHIFT_DB']

job = Job(glueContext)
job.init(args['JOB_NAME'], args)
print("STARTED")


#########################################
### UDF - User defined functions
#########################################


# Make text lowercase, remove punctuation and remove words containing numbers,remove emojis.
def clean_text(text):
    if text is None:
        return None
    text = re.sub(r'[%s]' % re.escape(string.punctuation), '', text.lower())
    text = re.sub(r'\w*\d\w*', '', text)
    re_emoji = re.compile(r'[\U00010000-\U0010ffff]', flags=re.UNICODE)
    text = re_emoji.sub(r'', text)
    text = re.sub(r'[^a-z] ', '', text)
    return text


get_clean_udf = F.udf(lambda l: clean_text(l), StringType())

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

input_df = input_df.select('available', 'best_seller_rank', 'brand', 'category', 'channel', 'discount',
                           'extracted_date', 'mrp', 'name', 'price', 'product_href', 'rating', 'rating_aspect',
                           'rating_count', 'rating_star', 'reviews_href', 'sku', 'sub_categories')

print(f"Reading data from {GLUE_TABLE}")
print(f"Size of input_df = {input_df.count()}")
input_df.show(5)

#########################################
### Load Mapping Tables to DFs (Read)
#########################################


# Create Dynamic Frames for Read RNR Category
category_df = glueContext.create_dynamic_frame.from_catalog(database=MAPPING_GLUE_DB,
                                                            table_name=CATEGORY_GLUE_TABLE).toDF()

# Create Dynamic Frames for Read RNR Products
products_df = glueContext.create_dynamic_frame.from_catalog(database=MAPPING_GLUE_DB,
                                                            table_name=PRODUCT_GLUE_TABLE).toDF()
products_df = products_df.drop("brand", "brand_id")

#########################################
### TRANSFORM (Modify)
#########################################

# Calculate rating map
input_df = input_df.withColumn("rating_star", F.from_json(F.col('rating_star'), MapType(StringType(), StringType()))) \
    .withColumn("rating_aspect", F.from_json(F.col('rating_aspect'), MapType(StringType(), StringType()))) \
    .withColumn("product_id", F.col('sku')) \
    .withColumn("id", F.col('sku')) \
    .withColumn("skuid", F.col('sku')) \
    .withColumn('year', F.year(F.to_timestamp('extracted_date', 'yyyyMMdd'))) \
    .withColumn('month', F.month(F.to_timestamp('extracted_date', 'yyyyMMdd'))) \
    .withColumn('day', F.dayofmonth(F.to_timestamp('extracted_date', 'yyyyMMdd'))) \
    .withColumn('sub_category', F.col('sub_categories')) \
    .withColumn('original_category', F.col('category'))\
    .withColumn('price', F.when(input_df.price.rlike("^(\d*).?(\d*)$"), input_df.price).otherwise('0'))\
    .withColumn('mrp', F.when(input_df.mrp.rlike("^(\d*).?(\d*)$"), input_df.mrp).otherwise('0'))\
    .withColumn('date', F.to_date('extracted_date', 'yyyyMMdd'))


# input_df = input_df.na.drop(subset=["name"])

input_df = input_df.select('available', 'best_seller_rank', 'brand', 'original_category', 'channel', 'discount',
                           'extracted_date', 'mrp', 'name', 'price', 'product_href', 'rating', 'rating_aspect',
                           'rating_count', 'rating_star', 'reviews_href', 'product_id', 'sub_category',
                           'year', 'month', 'day', 'id', 'skuid', 'date')

print(f"After cleaning data, size of input_df = {input_df.count()}")
input_df.show(5)

# Throw error if no data found after cleaning
if input_df.count() == 0:
    sys.exit(1)

########################################
## Category and Subcategory Mappings
########################################

mapped_df = input_df.join(products_df, "skuid", "INNER")
print(f"Size of mapped_df = {mapped_df.count()}")

unmapped_df = input_df.join(products_df, "skuid", "LEFTANTI")
print(f"Size of unmapped_df = {unmapped_df.count()}")

unmapped_df = unmapped_df.withColumn('sub_category', get_clean_udf(F.col('sub_category')))\
    .withColumn('original_category', get_clean_udf(F.col('original_category')))

# TODO: Add Alerts about non-mapped to sku, mapped with keyword, and not mapped
# df_na_keywords = unmapped_df.withColumn('category_list', F.array(F.col("sub_category"), F.col("original_category")))
# df_na_keywords = df_na_keywords.withColumn("raw_keyword", F.explode(F.col('category_list')).alias("raw_keyword"))
# df_na_keywords = df_na_keywords.join(category_df, "keyword", "leftouter")
# df_na_keywords = df_na_keywords.filter((F.col('category').isNull())
#                                  | (F.col('subcategory').isNull())
#                                  | (F.col('category') == '')
#                                  | (F.col('subcategory') == '')).select('keyword')
# df_na_keywords = df_na_keywords.dropDuplicates(subset=['keyword'])

unmapped_df.createOrReplaceTempView("unmapped_view")
category_df.createOrReplaceTempView("category_view")

unmapped_df = spark.sql(
    """
    SELECT 
        u.*,
        CASE WHEN c1.category = '' THEN null ELSE c1.category END AS c1_category,
        CASE WHEN c2.category = '' THEN null ELSE c2.category END AS c2_category,
        CASE WHEN c1.subcategory = '' THEN null ELSE c1.subcategory END AS c1_subcategory,
        CASE WHEN c2.subcategory = '' THEN null ELSE c2.subcategory END AS c2_subcategory
    FROM 
        unmapped_view AS u
    LEFT JOIN category_view AS c1 ON u.sub_category = c1.keyword
    LEFT JOIN category_view AS c2 ON u.original_category = c2.keyword
    """
)

cat_cols = [F.col('c1_category'), F.col('c2_category'), F.lit('NA')]
sub_cat_cols = [F.col('c1_subcategory'), F.col('c2_subcategory'), F.lit('NA')]
filter_not_null = "FILTER(_array, x -> x IS NOT NULL)"

unmapped_df = unmapped_df.withColumn("_array", F.array(*cat_cols)).withColumn("category", F.expr(filter_not_null).getItem(0)) \
    .withColumn("_array", F.array(*sub_cat_cols)).withColumn("sub_category", F.expr(filter_not_null).getItem(0)) \
    .drop("_array")

# Rearranging the required columns and dropping others
columns = ['available', 'best_seller_rank', 'brand', 'category', 'channel', 'discount',
           'extracted_date', 'mrp', 'name', 'price', 'product_href', 'rating', 'rating_aspect',
           'rating_count', 'rating_star', 'reviews_href', 'product_id', 'sub_category',
           'year', 'month', 'day', 'id', 'skuid', 'date']

mapped_df = mapped_df.withColumn('sub_category', F.col('subcategory')).select(columns)
unmapped_df = unmapped_df.select(columns)

# final df after Union of previously mapped and newly mapped category & subcategory
input_df = unmapped_df.union(mapped_df)
print(f"After category and subcategory mapping, size of input_df = {input_df.count()}")
input_df.show(5)

#########################################
### Prepare Tables
#########################################

# Create dataFrame for Product Table
product_table = input_df.select('available', 'best_seller_rank', 'brand', 'category', 'channel', 'discount', 'date',
                                'extracted_date', 'mrp', 'name', 'price', 'product_href', 'rating', 'rating_count',
                                'reviews_href', 'product_id', 'sub_category', 'year', 'month', 'day', 'id', 'skuid')

# Create dataFrame for Attribute Table with only STAR attributes
start_attribute_df = input_df.withColumn('attribute_type', F.lit('STAR')) \
    .select(
    F.explode(input_df['rating_star']),
    F.col('attribute_type'),
    F.col('product_id'),
    F.col('skuid'),
    F.col('brand'),
    F.col('channel'),
    F.col('category'),
    F.col('sub_category'),
    F.col('year'),
    F.col('month'),
    F.col('day'),
    F.col('date'),
    F.col('extracted_date')
)

# Create dataFrame for Attribute Table with only ASPECT attributes
aspect_attribute_df = input_df.withColumn('attribute_type', F.lit('ASPECT')) \
    .select(
    F.explode(input_df['rating_aspect']),
    F.col('attribute_type'),
    F.col('product_id'),
    F.col('skuid'),
    F.col('brand'),
    F.col('channel'),
    F.col('category'),
    F.col('sub_category'),
    F.col('year'),
    F.col('month'),
    F.col('day'),
    F.col('date'),
    F.col('extracted_date')
)

# Union STAR and ASPECT attribute tables
attribute_table = start_attribute_df.union(aspect_attribute_df)

# Throw error if not data found in product and attribute tables.
if product_table.count() == 0 or attribute_table.count() == 0:
    sys.exit(1)

print(f"Size of product_table = {product_table.count()}")
product_table.show(5)
print(f"Size of attribute_table = {attribute_table.count()}")
attribute_table.show(5)

#########################################
### Write to Redshift
#########################################

glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=DynamicFrame.fromDF(product_table, glueContext, 'product'),
    catalog_connection=REDSHIFT_DB,
    connection_options={
        "dbtable": "review_and_rating.product",
        "database": REDSHIFT_DB,
        "aws_iam_role": ROLE_ARN
    },
    redshift_tmp_dir=args["TempDir"]
)

glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=DynamicFrame.fromDF(attribute_table, glueContext, 'attribute'),
    catalog_connection=REDSHIFT_DB,
    connection_options={
        "dbtable": "review_and_rating.attribute",
        "database": REDSHIFT_DB,
        "aws_iam_role": ROLE_ARN
    },
    redshift_tmp_dir=args["TempDir"]
)

print("COMPLETED")
job.commit()
