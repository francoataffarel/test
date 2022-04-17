import logging
import json
from typing import List
from datetime import datetime
from scrapy import Request
from decouple import config
import requests

from ..base import AbstractMarketplace
from ...items import HotstylesItem
from ..utilities import ChannelType


class Ajio(AbstractMarketplace):
    smart_proxy = None

    def __init__(self, **kwargs) -> None:
        super().__init__()
        self.smart_proxy = config('PROXY_ROTATING', None) if kwargs['proxy'] else None

    @property
    def logger(self):
        logger = logging.getLogger(ChannelType.Ajio.name)
        return logging.LoggerAdapter(logger, {'hotstyles_spider': self})

    def start_requests(self, sku_ids: List[str]):
        self.print_config()
        for sku_id in sku_ids:
            response = requests.get(f"https://www.ajio.com/search/?text={sku_id}", allow_redirects=False)
            if response.status_code == 302:
                style_path = response.headers['Location']
                yield self.request(f"https://www.ajio.com/api{style_path}", self.extract_style, sku_id=sku_id, style_path=style_path)

    def extract_style(self, response, **kwargs):
        self.logger.info(f"Fetching style from api {response.url}")
        selection = json.loads(response.text)
        pid = kwargs["sku_id"]
        product_name = selection['name']
        product_href = f'https://www.ajio.com{kwargs["style_path"]}'
        price = str(int(selection['price']['value']))
        mrp = str(int(selection['wasPriceData']['value']))
        discount = str(int(mrp) - int(price))
        rating = '0'
        rating_count = str(selection['numberOfReviews'])

        category_1 = 'Fashion'
        category_2 = selection['brickSubCategory']
        category_3 = selection['brickName']
        category_4 = selection['brickCategory']
        rank_1 = ''
        rank_2 = ''
        rank_3 = ''
        rank_4 = ''
        extracted_date = datetime.today().strftime('%Y-%m-%d')

        yield HotstylesItem(skuid=pid, name=product_name, channel=ChannelType.Ajio,
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
