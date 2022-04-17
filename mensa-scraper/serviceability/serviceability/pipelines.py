# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import re
from datetime import datetime
from io import BytesIO
from urllib.parse import urlparse

# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from scrapy.utils.misc import load_object
import boto3
from botocore.exceptions import ClientError
from .spiders.utils import ChannelType

def get_channel_short_name(name):
    if name == ChannelType.AmazonIn:
        return 'amzin'
    elif name == ChannelType.Flipkart:
        return 'flip'
    elif name == ChannelType.Myntra:
        return 'myn'
    elif name == ChannelType.Ajio:
        return 'ajio'
    elif name == ChannelType.NykaaFashion:
        return 'nykf'
    elif name == ChannelType.Nykaa:
        return 'nyk'

class ServiceabilityPipeline:
    def process_item(self, item, spider):
        return item

class UploadError(Exception):
    pass

class S3Uploader:
    def __init__(self, settings):
        self.client = boto3.client(
            's3',
            region_name=settings['AWS_REGION_NAME'], use_ssl=settings['AWS_USE_SSL'],
            verify=settings['AWS_VERIFY'], endpoint_url=settings['AWS_ENDPOINT_URL'],
            aws_access_key_id=settings['AWS_ACCESS_KEY_ID'],
            aws_secret_access_key=settings['AWS_SECRET_ACCESS_KEY'])

    def upload_file_obj(self, f, bucket_name, object_key):
        try:
            self.client.upload_fileobj(f, bucket_name, object_key)
        except ClientError as ex:
            raise UploadError(ex)

class S3Pipeline:
    """
    Scrapy pipeline to store items into S3 bucket with JSONLines format.
    """

    def __init__(self, settings, stats):
        self.stats = stats

        s3_serviceability_url = settings['S3_SERVICEABILITY_URL']
        self.serviceability_bucket_name = urlparse(s3_serviceability_url).hostname
        self.serviceability_object_key = urlparse(s3_serviceability_url).path[1:]  # Remove the first '/'

        self.max_chunk_size = settings.getint('S3PIPELINE_MAX_CHUNK_SIZE', 2500)
        self.exporter_cls = load_object('scrapy.exporters.JsonLinesItemExporter')
        self.uploader = S3Uploader(settings)

        self.serviceability_items = []
        self.serviceability_chunk_number = 0

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings, crawler.stats)

    def process_item(self, item, spider):
        """
        Process single item. Add item to items and then upload to S3
        if size of items >= max_chunk_size.
        """
        self.stats.inc_value("items/product")
        self.serviceability_items.append(item)
        if len(self.serviceability_items) >= self.max_chunk_size:
            self._upload_chunk()
        return item

    def open_spider(self, spider):
        """
        Callback function when spider is open.
        """
        # Store timestamp to replace {time} in S3PIPELINE_URL
        self.ts = datetime.today().strftime('%Y%m%d')
        self._spider = spider

    def close_spider(self, spider):
        """
        Callback function when spider is closed.
        """
        # Upload remained items to S3.
        self._upload_chunk()

    def _upload_chunk(self):
        """
        Do upload items to S3.
        """
        if not self.serviceability_items:
            return  # Do nothing when items is empty.

        f = self._make_obj(self.serviceability_items)

        # Build object key by replacing variables in object key template.
        brand = ItemAdapter(self.serviceability_items[0]).get('brand').lower()
        channel = ItemAdapter(self.serviceability_items[0]).get('marketplace')
        channel = get_channel_short_name(channel)
        brand = re.sub(r'\W+', '', brand)
        params = {'chunk': self.serviceability_chunk_number, 'time': self.ts, 'name': f"{brand}-{channel}"}
        object_key = self.serviceability_object_key.format(**params)

        try:
            self.uploader.upload_file_obj(f, self.serviceability_bucket_name, object_key)
        except UploadError:
            self.stats.inc_value('pipeline/s3/fail')
            raise
        else:
            self.stats.inc_value('pipeline/s3/success')
        finally:
            # Prepare for the next chunk
            self.serviceability_chunk_number += len(self.serviceability_items)
            self.serviceability_items = []

    def _make_obj(self, items):
        """
        Build file object from items.
        """

        f = BytesIO()

        # Build file object using ItemExporter
        exporter = self.exporter_cls(f, encoding='utf-8')
        exporter.start_exporting()
        for item in items:
            exporter.export_item(item)
        exporter.finish_exporting()

        # Seek to the top of file to be read later
        f.seek(0)
        return f

