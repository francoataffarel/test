import os
import re

from decouple import config

import boto3
from scrapy.statscollectors import StatsCollector

regexes = ["log_count/.*", "pipeline/.*", "retry/.*", "items/.*", "downloader/.*", "scheduler/.*"]
combined_regex = "(" + ")|(".join(regexes) + ")"


class CloudWatchStatsCollector(StatsCollector):
    def __init__(self, crawler):
        super(CloudWatchStatsCollector, self).__init__(crawler)
        self.client = boto3.client(
            'cloudwatch',
            region_name=crawler.settings['AWS_REGION_NAME'], use_ssl=crawler.settings['AWS_USE_SSL'],
            verify=crawler.settings['AWS_VERIFY'], endpoint_url=crawler.settings['AWS_ENDPOINT_URL'],
            aws_access_key_id=crawler.settings['AWS_ACCESS_KEY_ID'],
            aws_secret_access_key=crawler.settings['AWS_SECRET_ACCESS_KEY'])

    @staticmethod
    def chunks(lst, n):
        """Yield successive n-sized chunks from lst."""
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    def _persist_stats(self, stats, spider):
        marketplace = spider.marketplace
        name = spider.name
        proxy = str(spider.proxy)
        env = config('ENV', default='dev')

        namespace = 'Discoverability Scraper'
        dimensions = [
            {'Name': 'MARKETPLACE', 'Value': marketplace},
            {'Name': 'PROXY', 'Value': proxy},
            {'Name': 'ENV', 'Value': env},
            {'Name': 'SPIDER_NAME', 'Value': name}
        ]

        data = []

        keys = [key for key in stats if re.match(combined_regex, key)]
        for key in keys:
            data.append({'MetricName': key, 'Dimensions': dimensions, 'Unit': 'Count', 'Value': stats[key]})

        data.append({'MetricName': 'elapsed_time_seconds', 'Dimensions': dimensions, 'Unit': 'Seconds',
                     'Value': stats.get('elapsed_time_seconds', 0)})

        for chunked_data in self.chunks(data, 10):
            self.client.put_metric_data(MetricData=chunked_data, Namespace=namespace)
