import logging
from datetime import datetime
from typing import List
from decouple import config
from scrapy import Request
import re
import json
from ..base import MarketPlaceFactory
from ..utils import ChannelType, get_query_param, replace_query_param, get_brand_formatted_name
from ...items import PricingItem

class Ajio(MarketPlaceFactory):
    smart_proxy = None
    scrape_limit = None
    category = None

    def __init__(self, category, **kwargs):
        super().__init__()
        self.category = category
        self.scrape_limit = kwargs['scrape_limit']
        self.smart_proxy = config('PROXY_ROTATING', None) if kwargs['proxy'] else None

    @property
    def logger(self):
        logger = logging.getLogger(ChannelType.Flipkart.name)
        return logging.LoggerAdapter(logger, {'pricing_spider': self})

    def start_requests(self, start_urls: List[str]):
        self.print_config()
        for url in start_urls:
            yield self.request(url, self.extract_list)

    def extract_list(self, response, **kwargs) -> List[str]:
        self.logger.info(f"Fetching style list from {response.url}")
        selection = json.loads(response.text)
        for product in selection['products']:
            pid = product['code']
            yield self.request(f"https://www.ajio.com/api/p/{pid}", self.extract_style)
        
        current_page = int(get_query_param(response.url, 'currentPage'))
        page_size = int(get_query_param(response.url, 'pageSize'))
        if (current_page + 1) * page_size <= self.scrape_limit and (current_page + 1) < selection['pagination']['totalPages']:
            url = replace_query_param(response.url, 'currentPage', current_page + 1)
            yield self.request(url, self.extract_list)
    
    def extract_style(self, response, **kwargs) -> PricingItem:
        self.logger.info(f"Fetching style from {response.url}")
        selection = json.loads(response.text)
        pid = response.url.split("/")[-1]
        brand = get_brand_formatted_name(selection['brandName'])
        name = selection['name']
        product_href = f'https://www.ajio.com/p/{pid}'
        available = selection['purchasable']
        price = str(int(selection['price']['value']))
        discount_percentage = str(selection['price']['discountValue'])
        mrp = str(int(selection['wasPriceData']['value']))
        discount = str(int(mrp) - int(price))
        extracted_date = datetime.today().strftime('%Y%m%d')
        coupon_offers_text = ", ".join([promos['description'] for promos in selection['potentialPromotions']]) \
                if len(selection['potentialPromotions']) > 0 else "NA"
        yield PricingItem(skuid=pid, category=self.category, brand=brand,
            title=name, product_href=product_href, mrp=mrp, price=price, discount=discount, 
            discount_percentage=discount_percentage, bank_offers="NA", coupon_offers=coupon_offers_text,
            extracted_date=extracted_date, available=available, marketplace=ChannelType.Ajio)        
        

    def request(self, url, callback, meta=None, **kwargs):
        if meta is None:
            meta = {}
        if self.smart_proxy is not None:
            meta['proxy'] = self.smart_proxy
        return Request(url, callback, meta=meta, cb_kwargs=kwargs)

    def print_config(self):
        self.logger.info("Starting crawler with configs")
        self.logger.info("Category: " + self.category)
        self.logger.info("scrape_limit: " + str(self.scrape_limit))
        self.logger.info("smart_proxy: " + str(self.smart_proxy))