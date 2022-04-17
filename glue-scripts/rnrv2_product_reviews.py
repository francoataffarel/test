import os
import sys
import boto3
from botocore.config import Config
from awsglue import DynamicFrame
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType, StructField
from pyspark.shell import spark
from botocore.exceptions import ClientError
import base64
import json

glueContext = GlueContext(SparkContext.getOrCreate())

args = getResolvedOptions(sys.argv, ['GLUE_DB', 'GLUE_TABLE', 'JOB_NAME', 'ROLE_ARN', 'REDSHIFT_DB', 'GLUE_SCHEDULER_DB', 'GLUE_SCHEDULER_TABLE'])
GLUE_DB = args['GLUE_DB']
GLUE_TABLE = args['GLUE_TABLE']
ROLE_ARN = args['ROLE_ARN']
REDSHIFT_DB = args['REDSHIFT_DB']
GLUE_SCHEDULER_DB = args['GLUE_SCHEDULER_DB']
GLUE_SCHEDULER_TABLE = args['GLUE_SCHEDULER_TABLE']

job = Job(glueContext)
job.init(args['JOB_NAME'], args)
print("STARTED")


#########################################
### User Defined Functions
#########################################

def get_sentiment_for_review(title, desc):
    config = Config(
        retries={
            'max_attempts': 10,
            'mode': 'standard'
        }
    )
    comprehend = boto3.client('comprehend', region_name='ap-south-1', config=config)
    title = '' if title is None else title
    desc = '' if desc is None else desc
    text = title + " " + desc
    try:
        response = comprehend.detect_sentiment(Text=text, LanguageCode='en')
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            return response['Sentiment']
        else:
            return 'POSITIVE'
    except (
            comprehend.exceptions.InvalidRequestException,
            comprehend.exceptions.TextSizeLimitExceededException,
            comprehend.exceptions.UnsupportedLanguageException,
            comprehend.exceptions.InternalServerException
    ) as e:
        print(e.response)
        return 'POSITIVE'


udf_get_sentiment_for_review = F.udf(get_sentiment_for_review, StringType())

#########################################
### Utility Functions
#########################################

def get_secrets(secrets_manager, secret_id):
        secret = None
        try:
            get_secret_value_response = secrets_manager.get_secret_value(SecretId=secret_id)
        except ClientError as e:
            print(e)
            raise Exception(str(e))
        else:
            # Decrypts secret using the associated KMS key.
            # Depending on whether the secret is a string or binary, one of these fields will be populated.
            if 'SecretString' in get_secret_value_response:
                secret = get_secret_value_response['SecretString']
            else:
                secret = base64.b64decode(get_secret_value_response['SecretBinary'])
        return secret

def get_formatted_review_date(review_date):
    return f'{review_date}T00:00:00'

def get_review_href(channel, id, skuid):
    if channel == 'Flipkart':
        return f'https://www.flipkart.com/reviews/{id}'
    elif channel == 'Amazon IN':
        return f'https://www.amazon.in/gp/customer-reviews/{id}'
    elif channel == 'Amazon COM':
        return f'https://www.amazon.com/gp/customer-reviews/{id}'
    elif channel == 'Amazon CA':
        return f'https://www.amazon.ca/gp/customer-reviews/{id}'
    elif channel == 'Amazon AE':
        return f'https://www.amazon.ae/gp/customer-reviews/{id}'
    elif channel == 'Myntra':
        return f'https://www.myntra.com/{skuid}'

def get_source_name(channel):
    if channel == 'Flipkart':
        return 'csflipkart'
    elif channel == 'Amazon COM':
        return 'csamazonus'
    elif channel == 'Amazon IN':
        return 'csamazonindia'
    elif channel == 'Amazon CA':
        return 'csamazonca'
    elif channel == 'Amazon AE':
        return 'csamazonuae'
    elif channel == 'Myntra':
        return 'csmyntra'

def raise_ticket(row):
    row = row.asDict()
    created_date = get_formatted_review_date(row['review_date'])
    response_body = {
        "name": row['author'],
        "message": row['description'],
        "title": row['title'],
        "postId": row['id'],
        "postLink": get_review_href(row['channel'], row['id'], row['skuid']),
        "createdOnGMTDate": created_date,
        "updatedOnGMTDate": created_date,
        "rating": row['star'],
        "maxRating": 5,
        "variant": "",
        "sourceId": "rv",
        "s_sourceName": get_source_name(row['channel']),
        "collectionName": row['skuid'],
        "b_verifiedPurchase": row['verified']
    }
    config = Config(
        retries={
            'max_attempts': 10,
            'mode': 'standard'
        }
    )    
    events = boto3.client('events', config=config)
    try:
        events.put_events(
            Entries=[
                {
                    'Source': 'mensa.rnr',
                    'DetailType': 'SimplifyReview-Tracker',
                    'Detail': json.dumps(response_body),
                }
            ]
        )
    except events.exceptions.InternalException as e:
        raise Exception(e.response)



#########################################
### Extract (Read)
#########################################

input_df = glueContext.create_dynamic_frame.from_catalog(database=GLUE_DB, table_name=GLUE_TABLE,
                                                         transformation_ctx="input_df")
input_df = input_df.toDF()

# If there is no data in input_df, exit the job with status success
if input_df.count() == 0:

    # Where there is no new incremental data, we need to delete contents of review_staging table
    # If not deleted, postetl job will send repetitive email of previous job run.
    # Hence, creating a empty dataframe with only one column (ID).
    # When inserting this empty dataframe, in pre_actions, we can delete contents of the review_staging table.
    pre_actions = "DROP TABLE IF EXISTS review_and_rating.review_staging;" \
                  "CREATE TABLE review_and_rating.review_staging AS SELECT * FROM review_and_rating.review WHERE 1=2;"

    review_table = spark.createDataFrame([], StructType([StructField('id', StringType())]))

    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=DynamicFrame.fromDF(review_table, glueContext, 'review'),
        catalog_connection=REDSHIFT_DB,
        connection_options={
            "dbtable": "review_and_rating.review_staging",
            "database": REDSHIFT_DB,
            "aws_iam_role": ROLE_ARN,
            "overwrite": "true",
            "preactions": pre_actions
        },
        redshift_tmp_dir=args["TempDir"]
    )
    print("No data. Exiting")
    job.commit()
    os._exit(os.EX_OK)

input_df = input_df.select('id', 'sku', 'brand', 'author', 'star', 'aspects', 'title', 'date', 'verified',
                           'description', 'extracted_date', 'channel', 'category', 'upvotes')

inhouse_df = glueContext.create_dynamic_frame.from_catalog(database=GLUE_SCHEDULER_DB, table_name=GLUE_SCHEDULER_TABLE)
inhouse_df = inhouse_df.toDF()

print(f"Reading data from {GLUE_TABLE}")
print(f"Size of input_df = {input_df.count()}")
input_df.show(5)

#########################################
### TRANSFORM (Modify)
#########################################

# Sentiment Analysis of the reviews
input_df = input_df.withColumn("sentiment", udf_get_sentiment_for_review(input_df.title, input_df.description))\
    .withColumn("review_date", F.to_date('date', 'yyyyMMdd'))\
    .withColumn("skuid", F.col('sku'))\
    .withColumn('date', F.to_date('extracted_date', 'yyyyMMdd'))\
    .dropDuplicates(subset=['id'])

print(f"After cleaning data, size of input_df = {input_df.count()}")
input_df.show(5)

#########################################
### Prepare Tables
#########################################

# Create dataFrame for Review Table
review_table = input_df.select('id', 'skuid', 'brand', 'author', 'star', 'aspects', 'title', 'review_date', 'verified',
                               'description', 'extracted_date', 'channel', 'category', 'upvotes', 'sentiment', 'date')

inhouse_brands_table = inhouse_df.select('name', 'project', 'active', 'brand_type')
inhouse_brand_table = inhouse_brands_table.filter((inhouse_brands_table.project == 'rnr') & (inhouse_brands_table.brand_type == 'Internal') & (inhouse_brands_table.active))
inhouse_brands_list = inhouse_brand_table.select('name').collect()
inhouse_brands_list = [item.name for item in inhouse_brands_list]
inhouse_df = review_table[review_table['brand'].isin(inhouse_brands_list)]
inhouse_df = inhouse_df[inhouse_df['channel'].isin(['Flipkart', 'Amazon COM', 'Amazon CA', 'Amazon AE', 'Amazon IN', 'Myntra'])]
inhouse_df.foreach(raise_ticket)

print(f"Size of review_table = {review_table.count()}")
review_table.show(5)

#########################################
### Load - Write to Redshift
#########################################

pre_actions = "DROP TABLE IF EXISTS review_and_rating.review_staging;" \
              "CREATE TABLE review_and_rating.review_staging AS SELECT * FROM review_and_rating.review WHERE 1=2;"

post_actions = "BEGIN;" \
               "DELETE FROM review_and_rating.review_staging USING review_and_rating.review AS S " \
               "WHERE review_and_rating.review_staging.id = S.id;" \
               "INSERT INTO review_and_rating.review SELECT * FROM review_and_rating.review_staging;" \
               "END;"

glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=DynamicFrame.fromDF(review_table, glueContext, 'review'),
    catalog_connection=REDSHIFT_DB,
    connection_options={
        "dbtable": "review_and_rating.review_staging",
        "database": REDSHIFT_DB,
        "aws_iam_role": ROLE_ARN,
        "overwrite": "true",
        "preactions": pre_actions,
        "postactions": post_actions
    },
    redshift_tmp_dir=args["TempDir"]
)

print("COMPLETED")
job.commit()
