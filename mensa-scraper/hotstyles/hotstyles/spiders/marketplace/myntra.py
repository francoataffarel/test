import logging
import json
from typing import List
from datetime import datetime
from scrapy import Request
from decouple import config

from ..base import AbstractMarketplace
from ...items import HotstylesItem
from ..utilities import ChannelType


class Myntra(AbstractMarketplace):
    smart_proxy = None

    def __init__(self, **kwargs) -> None:
        super().__init__()
        self.smart_proxy = config('PROXY_ROTATING', None) if kwargs['proxy'] else None

    @property
    def logger(self):
        logger = logging.getLogger(ChannelType.Myntra.name)
        return logging.LoggerAdapter(logger, {'hotstyles_spider': self})

    def start_requests(self, sku_ids: List[str]):
        self.print_config()
        start_urls = [f'https://www.myntra.com/{sku_ids[0]}']
        for url in start_urls:
            yield self.request(url, self.start_request_cookies_wrapper, sku_ids=sku_ids)

    def start_request_cookies_wrapper(self, response, **kwargs):
        self.logger.info(f"Referrer Response {response.url}")
        sku_ids = kwargs['sku_ids']
        for sku_id in sku_ids:
            url = f"https://www.myntra.com/gateway/v2/product/{sku_id}"
            yield self.request(url, self.extract_style, sku_id=sku_id)

    def extract_style(self, response, **kwargs):
        self.logger.info(f"Fetching style from {response.url}")
        selection = json.loads(response.text)
        if 'style' not in selection:
            return
        selection = selection['style']
        mid = str(selection['id'])
        product_name = selection['name']
        product_href = f"https://www.myntra.com/{selection['id']}"
        mrp = str(selection['mrp'])
        price = str(min(map(lambda item: item['sizeSellerData'][0]['discountedPrice'] if item['available'] and len(
            item['sizeSellerData']) > 0 else mrp, selection['sizes'])))

        discount = str(int(mrp) - int(price))

        rating = selection['ratings']['averageRating'] if selection['ratings'] else '0'
        rating_count = selection['ratings']['totalCount'] if selection['ratings'] else '0'

        category_1 = selection['analytics']['masterCategory'] if 'masterCategory' in selection['analytics'] else ''
        category_2 = selection['analytics']['subCategory'] if 'subCategory' in selection['analytics'] else ''
        category_3 = selection['analytics']['articleType'] if 'articleType' in selection['analytics'] else ''
        category_4 = selection['analytics']['gender'] if 'gender' in selection['analytics'] else ''
        rank_1 = ''
        rank_2 = ''
        rank_3 = ''
        rank_4 = ''
        extracted_date = datetime.today().strftime('%Y-%m-%d')

        yield HotstylesItem(skuid=mid, name=product_name, channel=ChannelType.Myntra,
                            product_href=product_href, extracted_date=extracted_date,
                            mrp=mrp, price=price, discount=discount, rating=rating, rating_count=rating_count,
                            category_1=category_1, rank_1=rank_1, category_2=category_2, rank_2=rank_2,
                            category_3=category_3, rank_3=rank_3, category_4=category_4, rank_4=rank_4
                            )

    def request(self, url, callback, meta=None, **kwargs):
        if meta is None:
            meta = {}
        if self.smart_proxy is not None:
            meta['proxy'] = self.smart_proxy
        return Request(url, callback, meta=meta, cb_kwargs=kwargs)

    def print_config(self):
        self.logger.info("Starting crawler with configs")
        self.logger.info("smart_proxy: " + str(self.smart_proxy))
