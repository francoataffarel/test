import os
import json
import logging
import boto3
import base64
from datetime import datetime
import calendar
from urllib.parse import urlencode
from botocore.config import Config
import psycopg2
import requests

from botocore.exceptions import ClientError
from scraper_job import RNRScraper, ServiceabilityScraper, DiscoverabilityScraper, BestSellersScraper, \
    BrokennessScraper, PricingScraper, HotstylesScraper

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class SchedulerTask:
    def get_scraper(self, scraper_name, marketplace, arguments, **kwargs):
        if scraper_name == 'rnr':
            return RNRScraper(marketplace, arguments, **kwargs)
        elif scraper_name == 'serviceability':
            return ServiceabilityScraper(marketplace, arguments, **kwargs)
        elif scraper_name == 'discoverability':
            return DiscoverabilityScraper(marketplace, arguments, **kwargs)
        elif scraper_name == 'bestsellers':
            return BestSellersScraper(marketplace, arguments, **kwargs)
        elif scraper_name == 'brokenness':
            return BrokennessScraper(marketplace, arguments, **kwargs)
        elif scraper_name == 'pricing':
            return PricingScraper(marketplace, arguments, **kwargs)
        elif scraper_name == 'hotstyles':
            return HotstylesScraper(marketplace, arguments, **kwargs)

    def get_secrets(self, secret_id):
        secret = None
        try:
            get_secret_value_response = self.secrets_manager.get_secret_value(SecretId=secret_id)
        except ClientError as e:
            logger.error(e)
            raise Exception(str(e))
        else:
            # Decrypts secret using the associated KMS key.
            # Depending on whether the secret is a string or binary, one of these fields will be populated.
            if 'SecretString' in get_secret_value_response:
                secret = get_secret_value_response['SecretString']
            else:
                secret = base64.b64decode(get_secret_value_response['SecretBinary'])
        return secret

    def get_db_connection(self):
        if self.db_secrets is None:
            logger.error("ERROR: Unexpected error: Could not retrieve DB Connection details from secrets manager.")
            raise Exception("ERROR: Unexpected error: Could not retrieve DB Connection details from secrets manager.")
        try:
            db_details = json.loads(self.db_secrets)
            return psycopg2.connect(
                host=db_details['host'], port=str(db_details['port']), user=db_details['username'],
                database=db_details['database'], password=db_details['password'],
                options=f"-c search_path=dbo,{db_details['schema']}",
                connect_timeout=5
            )
        except Exception as e:
            logger.error("ERROR: Unexpected error: Could not connect to Postgresql instance.")
            logger.error(e)
            raise Exception(str(e))

    def get_scraping_sql_query(self):
        return f"SELECT project,arguments,frequency,marketplace,name,weekdays FROM {self.tasks_table} WHERE active"

    def get_internalreview_sql_query(self):
        return f"SELECT project,arguments,frequency,marketplace,name,weekdays FROM {self.tasks_table} WHERE project='rnr' and brand_type='Internal' and active"

    def get_hotstyles_sql_query(self):
        today = datetime.now().strftime('%Y-%m-%d')
        return f"SELECT channel, 'sku_ids=' || string_agg(channel_skuid, ',') AS arguments FROM {self.hotstyles_table} WHERE added_date <= '{today}'::date AND expiry_date >= '{today}'::date GROUP BY 1;"

    @staticmethod
    def add_task(frequency, week_day, month_day, week_year, weekdays):
        """
        Return true if current date comes under daily, weekly, fortnightly or mothly
        """
        if frequency == 'Daily':
            return True
        if frequency == 'Weekly' and calendar.day_name[week_day] in weekdays:
            return True
        if frequency == 'Fortnightly' and (week_year % 2 == 0 and week_day == 6):
            return True
        if frequency == 'Monthly' and month_day == 1:
            return True
        if frequency == 'BiWeekly' and calendar.day_name[week_day] in weekdays:
            return True
        return False

    def get_scraping_tasks(self):
        """
        Returns the tasks based on frequency and current date
        Daily - return this task
        Weekly - If current day is Sunday then return this task
        Fortnightly - If current date is 1st or 16th then return this task
        Monthly - If current date is 1st of month then return this task 
        """
        with self.db_connection.cursor() as cursor:
            cursor.execute(self.get_scraping_sql_query())
            tasks = []
            date = datetime.now()
            week_day = date.weekday()
            week_year = date.isocalendar()[1]
            month_day = date.day
            for row in cursor:
                frequency = row[2]
                weekdays = row[5]
                if self.add_task(frequency, week_day, month_day, week_year, weekdays):
                    scraper_obj = self.get_scraper(scraper_name=row[0], marketplace=row[3], arguments=row[1], name=row[4])
                    tasks.append(scraper_obj)

            return tasks

    def get_reviewticket_tasks(self):
        """
        Returns the tasks based on frequency and current date
        Daily - return this task
        Weekly - If current day is Sunday then return this task
        Fortnightly - If current date is 1st or 16th then return this task
        Monthly - If current date is 1st of month then return this task 
        """
        with self.db_connection.cursor() as cursor:
            cursor.execute(self.get_internalreview_sql_query())
            tasks = []
            date = datetime.now()
            week_day = date.weekday()
            for row in cursor:
                weekdays = row[5]
                if week_day not in weekdays: #Run everyday except configured days
                    scraper_obj = self.get_scraper(scraper_name=row[0], marketplace=row[3], arguments=row[1], name=row[4], ticket_review_request=True)
                    tasks.append(scraper_obj)

            return tasks

    def get_hotstyles_tasks(self):
        """
        Returns the tasks based on frequency and current date
        Daily - return this task
        Weekly - If current day is Sunday then return this task
        Fortnightly - If current date is 1st or 16th then return this task
        Monthly - If current date is 1st of month then return this task
        """
        with self.db_connection.cursor() as cursor:
            cursor.execute(self.get_hotstyles_sql_query())
            tasks = []
            for row in cursor:
                scraper_obj = self.get_scraper(scraper_name='hotstyles', marketplace=row[0], arguments=row[1])
                tasks.append(scraper_obj)
            return tasks

    def send_scraper_request(self, payload, headers):
        try:
            response = requests.request("POST", self.scraper_url, headers=headers, data=urlencode(payload))
            logger.info(response.text)
        except Exception as e:
            logger.error(e)

    def send_emails_for_tasks(self, tasks):
        data = {
            "project": "Brand Analytics",
            "currentDate": datetime.today().strftime("%d-%m-%Y")
        }
        jobs = []
        for task in tasks:
            project = task.arguments_dict['project']
            spider = task.arguments_dict['spider']
            task_url = f'{os.getenv("SCRAPYDWEB_LOADBALANCER")}/1/log/stats/{project}/{spider}/{task.job_name}'
            jobs.append({
                "jobId": task.job_name,
                "jobLogUrl": task_url
            })
        data['jobs'] = jobs
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
                        'Source': 'scheduler.lambda',
                        'DetailType': 'Job Logs',
                        'Detail': json.dumps(data),
                    }
                ]
            )
        except events.exceptions.InternalException as e:
            logger.error(e)


    def run(self, tasks):
        if len(tasks) == 0:
            return
        
        scraper_details = json.loads(self.scraper_secrets)
        headers = {
            'Authorization': scraper_details['Authorization'],
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        for task in tasks:
            payload = task.arguments_dict
            payload['setting'] = task.settings
            payload['jobid'] = task.job_name
            self.send_scraper_request(payload, headers)
        
        self.send_emails_for_tasks(tasks)

    def __init__(self):
        self.ENV = os.getenv('ENV', 'dev')
        self.SECRET_NAME_POSTGRESQL_DB = os.getenv('SECRET_NAME_POSTGRESQL_DB')
        self.REGION_NAME = os.getenv('REGION_NAME')
        self.scraper_url = os.getenv('SCRAPER_URL')
        self.SECRET_NAME_SCRAPER_AUTH = os.getenv('SECRET_NAME_SCRAPER_AUTH')
        self.tasks_table = 'schedulerconfig'
        self.hotstyles_table = 'hotstyles'
        logger.info(f"Starting function on ENV={self.ENV}")
        session = boto3.session.Session()
        self.secrets_manager = session.client(service_name='secretsmanager', region_name=self.REGION_NAME)
        self.scraper_secrets = self.get_secrets(self.SECRET_NAME_SCRAPER_AUTH)
        self.db_secrets = self.get_secrets(self.SECRET_NAME_POSTGRESQL_DB)
        self.db_connection = self.get_db_connection()
        logger.info("SUCCESS: Connection to RDS Postgresql instance succeeded")


def handler(event, context):
    scheduler = None
    try:
        scheduler = SchedulerTask()
        scraping_tasks = scheduler.get_scraping_tasks()
        scheduler.run(scraping_tasks)
        internal_reviewticket_tasks = scheduler.get_reviewticket_tasks()
        scheduler.run(internal_reviewticket_tasks)
        hotstyles_tasks = scheduler.get_hotstyles_tasks()
        scheduler.run(hotstyles_tasks)
    except Exception as e:
        logger.error("ERROR: Unexpected error: Could not run the tasks.")
        logger.error(e)
        raise Exception(str(e))
    finally:
        if scheduler and scheduler.db_connection:
            scheduler.db_connection.close()