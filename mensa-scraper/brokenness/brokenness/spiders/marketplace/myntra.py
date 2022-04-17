import logging
import json
from datetime import datetime
from typing import List
from urllib.parse import urlparse, parse_qsl

from decouple import config
from scrapy import Request

from ..base import MarketPlaceFactory
from ..uitilies import replace_query_param, ChannelType
from ...items import BrokennessItem


class Myntra(MarketPlaceFactory):
    brand = None
    smart_proxy = None
    scrape_limit = None

    def __init__(self, brand, **kwargs):
        super().__init__()
        self.brand = brand
        self.scrape_limit = kwargs['scrape_limit']
        self.smart_proxy = config('PROXY_ROTATING', None) if kwargs['proxy'] else None

    @property
    def logger(self):
        logger = logging.getLogger(ChannelType.Myntra.name)
        return logging.LoggerAdapter(logger, {'brokenness_spider': self})

    def start_requests(self, start_urls: List[str]):
        self.print_config()
        for url in start_urls:
            yield self.request(url, self.start_request_cookies_wrapper)

    def start_request_cookies_wrapper(self, response, **kwargs):
        brand_path = response.url.split('/')[-1]
        url = f'https://www.myntra.com/gateway/v2/search/{brand_path}?p=1&rows=50&o=0&plaEnabled=false'
        yield self.request(url, self.extract_list)

    def extract_list(self, response, **kwargs):
        selection = json.loads(response.text)
        style_list = [f"https://www.myntra.com/gateway/v2/product/{product['productId']}"
                      for product in selection["products"]]

        self.logger.info(f"Fetching style list from {response.url} count: {len(style_list)}")
        for url in style_list:
            yield self.request(url, self.extract_style)

        total_items = selection['totalCount']
        url = response.url
        page = int(dict(parse_qsl(urlparse(url)[4])).get('p', 1))

        if total_items and page * 50 < total_items and page <= self.scrape_limit / 50:
            url = replace_query_param(url, 'p', page + 1)
            url = replace_query_param(url, 'o', (page * 50) - 1)
            yield self.request(url, self.extract_list)

    def extract_style(self, response, **kwargs):
        self.logger.info(f"Fetching style from {response.url}")
        product = json.loads(response.text)['style']
        skuid = str(product['id'])
        product_href = f"https://www.myntra.com/{skuid}"
        name = product['name']
        extracted_date = datetime.today().strftime('%Y%m%d')
        total_skus = [size['label'] for size in product['sizes']]
        available_skus = [size['label'] for size in product['sizes'] if size['available']]

        count_total_skus = len(total_skus)
        count_available_skus = len(available_skus)

        score = round((count_total_skus - count_available_skus) / count_total_skus, 2)
        total_skus = ', '.join(total_skus)
        available_skus = ', '.join(available_skus)

        yield BrokennessItem(brand=self.brand, channel=ChannelType.Myntra, name=name, skuid=skuid,
                               product_href=product_href, total_skus=total_skus, available_skus=available_skus,
                               score=score, extracted_date=extracted_date)

    def request(self, url, callback, meta=None):
        if meta is None:
            meta = {}
        if self.smart_proxy is not None:
            meta['proxy'] = self.smart_proxy
        return Request(url, callback, meta=meta)

    def print_config(self):
        self.logger.info("Starting crawler with configs")
        self.logger.info("Brand: " + self.brand)
        self.logger.info("scrape_limit: " + str(self.scrape_limit))
        self.logger.info("smart_proxy: " + str(self.smart_proxy))
