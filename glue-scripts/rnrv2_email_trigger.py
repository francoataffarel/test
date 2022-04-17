import json 
import os
import sys
import datetime

import boto3
from botocore.config import Config
from pyspark import SparkContext
from pyspark.shell import spark

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

glueContext = GlueContext(SparkContext.getOrCreate())
glueClient = boto3.client('glue', region_name='ap-south-1')

args = getResolvedOptions(sys.argv, ['GLUE_REDSHIFT_DB', 'REDSHIFT_REVIEW_TABLE', 'JOB_NAME', 'ROLE_ARN', 'ENV'])
GLUE_DB = args['GLUE_REDSHIFT_DB']
REVIEW_TABLE = args['REDSHIFT_REVIEW_TABLE']
ROLE_ARN = args['ROLE_ARN']
ENV = args['ENV']

job = Job(glueContext)
job.init(args['JOB_NAME'], args)
print("STARTED")

now = datetime.datetime.now()
currentPreviousWeek = now - datetime.timedelta(days=7)
previousWeek = currentPreviousWeek - datetime.timedelta(days=1)
previousPreviousWeek = previousWeek - datetime.timedelta(days=7)

configs = {

    "currentEndDate":now.strftime("%Y-%m-%d"),
    "currentStartDate":currentPreviousWeek.strftime("%Y-%m-%d"),
    "previousEndDate":previousWeek.strftime("%Y-%m-%d"),
    "previousStartDate":previousPreviousWeek.strftime("%Y-%m-%d"),
    "currentWeekRange":str(str(now.strftime("%d/%m/%y"))+" - "+str(currentPreviousWeek.strftime("%d/%m/%y"))),
    "previousWeekRange":str(str(previousWeek.strftime("%d/%m/%y"))+" - "+str(previousPreviousWeek.strftime("%d/%m/%y"))),
    "brands" : ('Anubhutee','Dennis Lingo','Folkulture','Helea','HubberHolme','Ishin','Lilpicks','Priyaasi','Villain','Dellinger','Botanic Hearth','Majestic Pure','High Star','Estalon','TrustBasket')
}
images = {    
    "Anubhutee":"https://mensa-brands-logo.s3.ap-south-1.amazonaws.com/Anubhutee/Anubhutee_320_X_70.jpg",
    "Dennis Lingo":"https://mensa-brands-logo.s3.ap-south-1.amazonaws.com/Dennis+Lingo/dennis_lingo_320_X_70.png",
    "Hubberholme":"https://mensa-brands-logo.s3.ap-south-1.amazonaws.com/HubberHolme/HubberHolme_320_X_70.jpg",
    "Priyaasi":"https://mensa-brands-logo.s3.ap-south-1.amazonaws.com/Priyaasi/priyassi_320_X_70.png",
    "Villain":"https://mensa-brands-logo.s3.ap-south-1.amazonaws.com/Villain/villain_logo_320_X_70.png",
    "Ishin":"https://mensa-brands-logo.s3.ap-south-1.amazonaws.com/Ishin/ISHIN_320_X_70.png",
    "Folkulture":"https://mensa-brands-logo.s3.ap-south-1.amazonaws.com/Folkulture/folkulture_320_X_70.png",
    "Helea":"https://mensa-brands-logo.s3.ap-south-1.amazonaws.com/Helea/HELEA_320_X_70.png",
    "Lilpicks":"https://mensa-brands-logo.s3.ap-south-1.amazonaws.com/Lilpicks/lilpicks_logo_320_X_70.png",
    "Botanic Hearth":"https://mensa-brands-logo.s3.ap-south-1.amazonaws.com/Botanic+Hearth/botanic_320_X_70.png",
    "Dellinger":"https://mensa-brands-logo.s3.ap-south-1.amazonaws.com/Dellinger/delinger_320_X_70.jpeg",
    "Estalon":"https://mensa-brands-logo.s3.ap-south-1.amazonaws.com/Estalon/estalon_320_X_70.png",
    "High Star":"https://mensa-brands-logo.s3.ap-south-1.amazonaws.com/High+Star/high_star_320_X_70.jpeg",
    "Majestic Pure":"https://mensa-brands-logo.s3.ap-south-1.amazonaws.com/Majestic+Pure/majestic-pure-logo_320_X_70.png",
    "TrustBasket":"https://mensa-brands-logo.s3.ap-south-1.amazonaws.com/TrustBasket/trustbasket_320_X_70.png"
}

def push_to_heimdall(data):
    data = data.asDict()
    data['ENV'] = ENV
    data['imgUrl'] = images[data['Brand']]
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
                    'DetailType': 'Customer rating',
                    'Detail': json.dumps(data),
                }
            ]
        )
    except events.exceptions.InternalException as e:
        print(e.response)


review_df = glueContext.create_dynamic_frame.from_catalog(
    database=GLUE_DB, table_name=REVIEW_TABLE,
    redshift_tmp_dir=args["TempDir"], additional_options={"aws_iam_role": ROLE_ARN}
)
review_df = review_df.toDF()

# If there is no data in input_df, exit the job with status success
if review_df.count() == 0:
    print("No data. Exiting")
    job.commit()
    os._exit(os.EX_OK)

review_df = review_df.select('id', 'skuid', 'title', 'star', 'sentiment', 'description', 'brand', 'category',
                             'review_date', 'verified', 'upvotes', 'author', 'aspects', 'extracted_date', 'channel')

review_df.createOrReplaceTempView("view_review_df")
sql_df = spark.sql(
    """
    with all_data as (
	SELECT *
  	FROM view_review_df
  	WHERE brand in {brands}
    ) SELECT
    cw.brand as Brand,
    CAST('{currentWeekRange}' AS STRING) AS CurrentDate,
    CAST('{previousWeekRange}' AS STRING) AS PreviousDate,
    IFNULL(CAST(COUNT(DISTINCT(cw.id)) AS DECIMAL(10,2)),'NA') AS CurrentTotalReviews,
    IFNULL(CAST(COUNT(DISTINCT(pw.id)) AS DECIMAL(10,2)),'NA') AS PreviousTotalReviews,
	IFNULL(CAST(COUNT(DISTINCT(CASE WHEN cw.sentiment = 'POSITIVE' then cw.id end)) AS DECIMAL(10,2)),'NA') AS CurrentPositiveReviews,
    IFNULL(CAST((COUNT(DISTINCT(CASE WHEN cw.sentiment = 'POSITIVE' then cw.id end))*100)/COUNT(DISTINCT(cw.id)) AS DECIMAL(10,2)),'NA') AS CurrentPositivePercent,
	IFNULL(CAST(COUNT(DISTINCT(CASE WHEN cw.sentiment = 'NEGATIVE' then cw.id end)) AS DECIMAL(10,2)),'NA') AS CurrentNegativeReviews,
	IFNULL(CAST((COUNT(DISTINCT(CASE WHEN cw.sentiment = 'NEGATIVE' then cw.id end))*100)/COUNT(DISTINCT(cw.id)) AS DECIMAL(10,2)),'NA') AS CurrentNegativePercent,
	IFNULL(CAST(COUNT(DISTINCT(CASE WHEN cw.star = 5.0 Or cw.star = 5 THEN cw.id end)) AS DECIMAL(10,2)),'NA') AS CurrentFiveStarReview,
	IFNULL(CAST((COUNT(DISTINCT(CASE WHEN cw.star = 5.0 Or cw.star = 5 THEN cw.id end))*100)/COUNT(DISTINCT(cw.id)) AS DECIMAL(10,2)),'NA') AS CurrentFiveStarReviewsPercent,
	IFNULL(CAST((COUNT(DISTINCT(CASE WHEN cw.star = 4.0 Or cw.star = 4 THEN cw.id end))*100)/COUNT(DISTINCT(cw.id)) AS DECIMAL(10,2)),'NA') AS CurrentFourStarReviewsPercent,
	IFNULL(CAST((COUNT(DISTINCT(CASE WHEN cw.star = 3.0 Or cw.star = 3 THEN cw.id end))*100)/COUNT(DISTINCT(cw.id)) AS DECIMAL(10,2)),'NA') AS CurrentThreeStarReviewsPercent,
	IFNULL(CAST((COUNT(DISTINCT(CASE WHEN cw.star = 2.0 Or cw.star = 2 THEN cw.id end))*100)/COUNT(DISTINCT(cw.id)) AS DECIMAL(10,2)),'NA') AS CurrentTwoStarReviewsPercent,
	IFNULL(CAST((COUNT(DISTINCT(CASE WHEN cw.star = 1.0 Or cw.star = 1 THEN cw.id end))*100)/COUNT(DISTINCT(cw.id)) AS DECIMAL(10,2)),'NA') AS CurrentOneStarReviewsPercent,
	IFNULL(CAST(COUNT(DISTINCT(CASE WHEN pw.sentiment = 'POSITIVE' then pw.id end)) AS DECIMAL(10,2) ),'NA') AS PreviousPositiveReviews,
    IFNULL(CAST((COUNT(DISTINCT(CASE WHEN pw.sentiment = 'POSITIVE' then pw.id end))*100)/COUNT(DISTINCT(pw.id)) AS DECIMAL(10,2)),'NA') AS PreviousPositivePercent,
	IFNULL(CAST(COUNT(DISTINCT(CASE WHEN pw.sentiment = 'NEGATIVE' then pw.id end)) AS DECIMAL(10,2)),'NA') AS PreviousNegativeReviews,
	IFNULL(CAST((COUNT(DISTINCT(CASE WHEN pw.sentiment = 'NEGATIVE' then pw.id end))*100)/COUNT(DISTINCT(pw.id)) AS DECIMAL(10,2)),'NA') AS PreviousNegativePercent,
	IFNULL(CAST(COUNT(DISTINCT(CASE WHEN pw.star = 5.0 Or pw.star = 5 THEN pw.id end)) AS DECIMAL(10,2)),'NA') AS PreviousFiveStarReview,
	IFNULL(CAST((COUNT(DISTINCT(CASE WHEN pw.star = 5.0 Or pw.star = 5 THEN pw.id end))*100)/COUNT(DISTINCT(pw.id)) AS DECIMAL(10,2)),'NA') AS PreviousFiveStarReviewsPercent,
	IFNULL(CAST((COUNT(DISTINCT(CASE WHEN pw.star = 4.0 Or pw.star = 4 THEN pw.id end))*100)/COUNT(DISTINCT(pw.id)) AS DECIMAL(10,2)),'NA') AS PreviousFourStarReviewsPercent,
	IFNULL(CAST((COUNT(DISTINCT(CASE WHEN pw.star = 3.0 Or pw.star = 3 THEN pw.id end))*100)/COUNT(DISTINCT(pw.id)) AS DECIMAL(10,2)),'NA') AS PreviousThreeStarReviewsPercent,
	IFNULL(CAST((COUNT(DISTINCT(CASE WHEN pw.star = 2.0 Or pw.star = 2 THEN pw.id end))*100)/COUNT(DISTINCT(pw.id)) AS DECIMAL(10,2)),'NA') AS PreviousTwoStarReviewsPercent,
	IFNULL(CAST((COUNT(DISTINCT(CASE WHEN pw.star = 1.0 Or pw.star = 1 THEN pw.id end))*100)/COUNT(DISTINCT(pw.id)) AS DECIMAL(10,2)),'NA') AS PreviousOneStarReviewsPercent
    FROM 
        all_data cw
    LEFT JOIN (
      SELECT * FROM all_data AS vvd
      WHERE (vvd.review_date <= CAST('{previousEndDate}' AS STRING) AND vvd.review_date > CAST('{previousStartDate}' AS STRING))
    ) AS pw
    ON cw.brand = pw.brand
    WHERE 
        (cw.review_date < CAST('{currentEndDate}' AS STRING) AND cw.review_date >= CAST('{currentStartDate}' AS STRING))
    GROUP BY 
        cw.brand
""".format(**configs))
sql_df.show()

cumulative_sql_df = spark.sql(
    """
    with all_data as (
	SELECT *
  	FROM view_review_df
  	WHERE brand in {brands}
    ) SELECT
    cw.brand as Brand,
    IFNULL(CAST(AVG(cw.star) AS DECIMAL(10,2)),'NA') AS CurrentAvgRating,
    IFNULL(CAST(AVG(pw.star) AS DECIMAL(10,2)),'NA') AS PreviousAvgRating
    FROM 
        all_data cw
    LEFT JOIN (
      SELECT * FROM all_data AS vvd
      WHERE (vvd.review_date <= CAST('{previousEndDate}' AS STRING))
    ) AS pw
    ON cw.brand = pw.brand
    WHERE 
        (cw.review_date < CAST('{currentEndDate}' AS STRING))
    GROUP BY 
        cw.brand
""".format(**configs))
cumulative_sql_df.show()

sql_df = sql_df.join(cumulative_sql_df, "Brand")
sql_df.show()
sql_df.foreach(push_to_heimdall)

print("COMPLETED")
job.commit()
