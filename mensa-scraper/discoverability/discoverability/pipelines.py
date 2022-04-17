# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import re
from datetime import datetime
from io import BytesIO
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError
from itemadapter import ItemAdapter
from scrapy.utils.misc import load_object
from .spiders.utilities import ChannelType


def get_item_type(item):
    if type(item).__name__ == 'DiscoverabilityStyleItem':
        return 'search_product'
    elif type(item).__name__ == 'BrowseStyleItem':
        return 'browse_product'


def get_channel_short_name(name):
    if name == ChannelType.AmazonIn:
        return 'amzin'
    elif name == ChannelType.AmazonCom:
        return 'amzcom'
    elif name == ChannelType.Flipkart:
        return 'flip'
    elif name == ChannelType.Myntra:
        return 'myn'
    elif name == ChannelType.Ajio:
        return 'ajio'
    elif name == ChannelType.Nykaa:
        return 'nyk'
    elif name == ChannelType.NykaaFashion:
        return 'nykf'


class DiscoverabilityPipeline:
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

        s3_search_url = settings['S3_DISCOVERABILITY_SEARCH_URL']
        s3_browse_url = settings['S3_DISCOVERABILITY_BROWSE_URL']
        self.search_bucket_name = urlparse(s3_search_url).hostname
        self.search_object_key = urlparse(s3_search_url).path[1:]  # Remove the 1st '/'
        self.browse_bucket_name = urlparse(s3_browse_url).hostname
        self.browse_object_key = urlparse(s3_browse_url).path[1:]  # Remove the 1st '/'

        self.max_chunk_size = settings.getint('S3PIPELINE_MAX_CHUNK_SIZE', 2500)
        self.exporter_cls = load_object('scrapy.exporters.JsonLinesItemExporter')
        self.uploader = S3Uploader(settings)

        self.search_items = []
        self.browse_items = []
        self.item_chunk_number = 0

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings, crawler.stats)

    def process_item(self, item, spider):
        """
        Process single item. Add item to items and then upload to S3
        if size of items >= max_chunk_size.
        """
        item_type = get_item_type(item)
        if item_type == 'search_product':
            self.stats.inc_value("items/search_product")
            self.search_items.append(item)
            if len(self.search_items) >= self.max_chunk_size:
                self._upload_chunk(item_type)
        elif item_type == 'browse_product':
            self.stats.inc_value("items/browse_product")
            self.browse_items.append(item)
            if len(self.browse_items) >= self.max_chunk_size:
                self._upload_chunk(item_type)

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
        self._upload_chunk('search_product')
        self._upload_chunk('browse_product')

    def _upload_chunk(self, item_type):
        """
        Do upload items to S3.
        """
        if item_type == 'search_product':
            self._upload_chunk_search_products()
        elif item_type == 'browse_product':
            self._upload_chunk_browse_products()

    def _upload_chunk_search_products(self):
        """
        Do upload items to discoverability search products S3 bucket.
        """
        if not self.search_items:
            return  # Do nothing when items is empty.

        f = self._make_obj(self.search_items)

        # Build object key by replacing variables in object key template.
        # keeping a character limit of 25 for the keyword for object name
        keyword = '-'.join(ItemAdapter(self.search_items[0]).get('keyword').lower().split())[:25]
        channel = ItemAdapter(self.search_items[0]).get('channel')
        sort = ItemAdapter(self.search_items[0]).get('sort')
        channel = get_channel_short_name(channel)
        keyword = re.sub(r'\W+', '', keyword)
        params = {'chunk': self.item_chunk_number, 'time': self.ts, 'name': f"{channel}-{sort}-{keyword}"}
        object_key = self.search_object_key.format(**params)

        try:
            self.uploader.upload_file_obj(f, self.search_bucket_name, object_key)
        except UploadError:
            self.stats.inc_value('pipeline/s3/fail')
            raise
        else:
            self.stats.inc_value('pipeline/s3/success')
        finally:
            # Prepare for the next chunk
            self.item_chunk_number += len(self.search_items)
            self.items = []

    def _upload_chunk_browse_products(self):
        """
        Do upload items to discoverability browse products S3 bucket.
        """
        if not self.browse_items:
            return  # Do nothing when items is empty.

        f = self._make_obj(self.browse_items)

        # Build object key by replacing variables in object key template.
        # keeping a character limit of 25 for the keyword for object name
        offer_type = '-'.join(ItemAdapter(self.browse_items[0]).get('offer_type').lower().split())[:12]
        offer_title = '-'.join(ItemAdapter(self.browse_items[0]).get('offer_title').lower().split())[:12]
        channel = ItemAdapter(self.browse_items[0]).get('channel')
        channel = get_channel_short_name(channel)
        offer_type = re.sub(r'\W+', '', offer_type)
        offer_title = re.sub(r'\W+', '', offer_title)
        params = {'chunk': self.item_chunk_number, 'time': self.ts, 'name': f"{channel}-{offer_type}-{offer_title}"}
        object_key = self.browse_object_key.format(**params)

        try:
            self.uploader.upload_file_obj(f, self.browse_bucket_name, object_key)
        except UploadError:
            self.stats.inc_value('pipeline/s3/fail')
            raise
        else:
            self.stats.inc_value('pipeline/s3/success')
        finally:
            # Prepare for the next chunk
            self.item_chunk_number += len(self.browse_items)
            self.items = []

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
