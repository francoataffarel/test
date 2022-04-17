import logging
import re
import json
from datetime import datetime, timedelta
from typing import List
from urllib.parse import urlparse, parse_qsl

from scrapy import Request
from decouple import config

from ..base import AbstractMarketplace
from ..utilities import ChannelType
from ...items import HotstylesItem


def build_style_list_urls(sku_ids: List[str]) -> List[str]:
    return [f'https://www.flipkart.com/product/p/itme?pid={skuid}' for skuid in sku_ids]


class Flipkart(AbstractMarketplace):
    smart_proxy = None

    def __init__(self, **kwargs) -> None:
        super().__init__()
        self.smart_proxy = config('PROXY_ROTATING', None) if kwargs['proxy'] else None

    @property
    def logger(self):
        logger = logging.getLogger(ChannelType.Flipkart.name)
        return logging.LoggerAdapter(logger, {'hotstyles_spider': self})

    def start_requests(self, sku_ids: List[str]):
        self.print_config()
        start_urls = build_style_list_urls(sku_ids)
        for url in start_urls:
            yield self.request(url, self.extract_style)

    def extract_style(self, response, **kwargs):
        self.logger.info(f"Fetching style from {response.url}")
        selection = response
        fsn = dict(parse_qsl(urlparse(response.url)[4]))['pid']

        product_name = selection.xpath(".//div[contains(concat(' ',normalize-space(@class),' '),' aMaAEs "
                                       "')]//h1//span[contains(concat(' ',normalize-space(@class),' '),"
                                       "' B_NuCI ')]/text()").get()
        product_href = response.url
        mrp = selection.xpath(".//div[contains(concat(' ',normalize-space(@class),' '),' CEmiEU ')]"
                              "//div[contains(concat(' ',normalize-space(@class),' '),' _3I9_wc ')]/text()").getall()
        price = selection.xpath(".//div[contains(concat(' ',normalize-space(@class),' '),' CEmiEU ')]"
                                "//div[contains(concat(' ',normalize-space(@class),' '),' _30jeq3 ')]/text()").get()
        rating = selection.xpath("//div[contains(concat(' ',normalize-space(@class),' '),' _1YokD2 ')]"
                                 "//span[contains(@id,'productRating')]//div/text()").get()
        rating_count = selection.xpath("//span/text()[contains(., 'ratings and') and contains(., 'reviews') ]").get()

        if price is not None:
            price = re.sub(r'\D', '', price)
        else:
            price = '0'

        if mrp:
            mrp = ''.join(mrp)
            mrp = re.sub(r'\D', '', mrp)
        else:
            mrp = price

        if mrp is not None and price is not None:
            discount = str(int(mrp) - int(price))
        else:
            discount = '0'

        if rating_count is not None:
            rating_count = re.sub(r'\D', '', rating_count)
        else:
            rating_count = '0'

        rating = rating if rating else '0'
        sub_category = selection.xpath(".//div[contains(concat(' ',normalize-space(@class),' '),' "
                                       "_1MR4o5 ')]//a/text()").getall()
        category_1 = sub_category[1].strip() if sub_category is not None and len(sub_category) > 1 else ""
        category_2 = sub_category[2].strip() if sub_category is not None and len(sub_category) > 2 else ""
        category_3 = sub_category[3].strip() if sub_category is not None and len(sub_category) > 3 else ""
        category_4 = sub_category[4].strip() if sub_category is not None and len(sub_category) > 4 else ""
        rank_1 = ''
        rank_2 = ''
        rank_3 = ''
        rank_4 = ''

        extracted_date = datetime.today().strftime('%Y-%m-%d')

        yield HotstylesItem(skuid=fsn, name=product_name, channel=ChannelType.Flipkart,
                            product_href=product_href, extracted_date=extracted_date,
                            mrp=mrp, price=price, discount=discount, rating=rating, rating_count=rating_count,
                            category_1=category_1, rank_1=rank_1, category_2=category_2, rank_2=rank_2,
                            category_3=category_3, rank_3=rank_3, category_4=category_4, rank_4=rank_4
                            )

    def request(self, url, callback, meta=None):
        if meta is None:
            meta = {}
        if self.smart_proxy is not None:
            meta['proxy'] = self.smart_proxy
        return Request(url, callback, meta=meta)

    def print_config(self):
        self.logger.info("Starting crawler with configs")
        self.logger.info("smart_proxy: " + str(self.smart_proxy))

