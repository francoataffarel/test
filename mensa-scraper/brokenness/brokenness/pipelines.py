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
from .spiders.uitilies import ChannelType
from .items import BrokennessItem


class BrokennessPipeline:
    def process_item(self, item, spider):
        return item


def get_channel_short_name(name):
    if name == ChannelType.AmazonIn:
        return 'amzin'
    elif name == ChannelType.AmazonCom:
        return 'amzcom'
    elif name == ChannelType.AmazonAe:
        return 'amzae'
    elif name == ChannelType.Flipkart:
        return 'flip'
    elif name == ChannelType.Myntra:
        return 'myn'
    elif name == ChannelType.Nykaa:
        return 'nyk'
    elif name == ChannelType.NykaaFashion:
        return 'nykf'


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

        s3_brokenness_url = settings['S3_BROKENNESS_URL']
        self.bucket_name = urlparse(s3_brokenness_url).hostname
        self.object_key = urlparse(s3_brokenness_url).path[1:]  # Remove the first '/'

        self.exporter_cls = load_object('scrapy.exporters.JsonLinesItemExporter')
        self.uploader = S3Uploader(settings)

        self.items = []
        self.amazon_items = []

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings, crawler.stats)

    def process_item(self, item, spider):
        """
        Process single item. Add item to items
        """
        self.stats.inc_value("items/product")

        # If item is from Amazon channels, then add to amazon_items else items
        if ItemAdapter(item).get('channel') in ['Amazon IN', 'Amazon COM', 'Amazon AE']:
            self.amazon_items.append(item)
        else:
            self.items.append(item)

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
        # First upload items from all channels except Amazon's
        self._upload_chunk(self.items)
        # Then prepare the data for amazon items and then upload it.
        self._upload_chunk(self.prepare_amazon_items(self.amazon_items))

    def _upload_chunk(self, items):
        """
        Do upload items to S3.
        """
        if not items:
            return  # Do nothing when items is empty.

        f = self._make_obj(items)

        # Build object key by replacing variables in object key template.
        brand = ItemAdapter(items[0]).get('brand').lower()
        channel = ItemAdapter(items[0]).get('channel')
        channel = get_channel_short_name(channel)
        brand = re.sub(r'\W+', '', brand)
        params = {'chunk': 0, 'time': self.ts, 'name': f"{brand}-{channel}"}
        object_key = self.object_key.format(**params)

        try:
            self.uploader.upload_file_obj(f, self.bucket_name, object_key)
        except UploadError:
            self.stats.inc_value('pipeline/s3/fail')
            raise
        else:
            self.stats.inc_value('pipeline/s3/success')

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

    def prepare_amazon_items(self, items):
        # Map to combine all size data according to parent data
        skuid_map = {}

        # Create list of Brokenness items which needs to be returned
        items_to_return = []

        # Loop over Amazon's items
        for item in items:
            # Fetch the data using ItemAdapter
            skuid = ItemAdapter(item).get("skuid")
            total_skus = ItemAdapter(item).get("total_skus")
            available_skus = ItemAdapter(item).get("available_skus")

            # If skuid already exists in map, then combine total_sku and available_skus with this new item.
            # then just add this new item to map.
            if skuid in skuid_map:
                total_skus = skuid_map[skuid]["total_skus"] + total_skus
                available_skus = skuid_map[skuid]["available_skus"] + available_skus

            skuid_map.update({
                skuid: {
                    "total_skus": total_skus, "available_skus": available_skus,
                    "product_href": ItemAdapter(item).get("product_href"),
                    "name": ItemAdapter(item).get("name"), "channel": ItemAdapter(item).get("channel"),
                    "extracted_date": ItemAdapter(item).get("extracted_date"), "brand": ItemAdapter(item).get("brand")
                }
            })

        # Loop over map items where all size data is merge w.r.t parent_asin
        for skuid in skuid_map:
            # Remove duplicates from list
            total_skus = list(set(skuid_map[skuid]["total_skus"]))
            available_skus = list(set(skuid_map[skuid]["available_skus"]))

            # Score calculation
            count_total_skus = len(total_skus)
            count_available_skus = len(available_skus)

            score = round((count_total_skus - count_available_skus) / count_total_skus, 2)
            total_skus = ', '.join(total_skus)
            available_skus = ', '.join(available_skus)

            # Create brokenness item and add it to the list
            items_to_return.append(
                BrokennessItem(brand=skuid_map[skuid]["brand"], channel=skuid_map[skuid]["channel"],
                               name=skuid_map[skuid]["name"], skuid=skuid, score=score,
                               product_href=skuid_map[skuid]["product_href"], total_skus=total_skus,
                               available_skus=available_skus, extracted_date=skuid_map[skuid]["extracted_date"])
            )

        return items_to_return
